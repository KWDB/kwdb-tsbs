package pgconn

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
)

// KWDBTSStatementDescription describes a KWDB TS insert layout returned by the extended protocol.
type KWDBTSStatementDescription struct {
	Name       string
	TableName  string
	ParamOIDs  []uint32
	TagIndex   int16
	PtagIDs    []uint16
	StorageLen []uint32
}

func (sd *KWDBTSStatementDescription) StatementDescription() *StatementDescription {
	if sd == nil {
		return nil
	}

	desc := &StatementDescription{
		Name:      sd.Name,
		TableName: sd.TableName,
		TagIndex:  sd.TagIndex,
	}
	if len(sd.ParamOIDs) > 0 {
		desc.ParamOIDs = append([]uint32(nil), sd.ParamOIDs...)
	}
	if len(sd.PtagIDs) > 0 {
		desc.PtagIDs = append([]uint16(nil), sd.PtagIDs...)
	}
	if len(sd.StorageLen) > 0 {
		desc.StorageLen = append([]uint32(nil), sd.StorageLen...)
	}
	return desc
}

func kwdbTSStatementDescriptionFromStatement(sd *StatementDescription) *KWDBTSStatementDescription {
	if sd == nil {
		return nil
	}

	desc := &KWDBTSStatementDescription{
		Name:      sd.Name,
		TableName: sd.TableName,
		TagIndex:  sd.TagIndex,
	}
	if len(sd.ParamOIDs) > 0 {
		desc.ParamOIDs = append([]uint32(nil), sd.ParamOIDs...)
	}
	if len(sd.PtagIDs) > 0 {
		desc.PtagIDs = append([]uint16(nil), sd.PtagIDs...)
	}
	if len(sd.StorageLen) > 0 {
		desc.StorageLen = append([]uint32(nil), sd.StorageLen...)
	}
	return desc
}

func (pgConn *PgConn) PrepareKWDBTS(ctx context.Context, name, tableName string) (*KWDBTSStatementDescription, error) {
	if err := pgConn.lock(); err != nil {
		return nil, err
	}
	defer pgConn.unlock()

	if ctx != context.Background() {
		select {
		case <-ctx.Done():
			return nil, newContextAlreadyDoneError(ctx)
		default:
		}
		pgConn.contextWatcher.Watch(ctx)
		defer pgConn.contextWatcher.Unwatch()
	}

	pgConn.frontend.SendParseEx(&pgproto3.ParseEx{Name: name, TableName: tableName})
	pgConn.frontend.SendDescribe(&pgproto3.Describe{ObjectType: 'S', Name: name})
	pgConn.frontend.SendSync(&pgproto3.Sync{})
	if err := pgConn.frontend.Flush(); err != nil {
		pgConn.asyncClose()
		return nil, err
	}

	psd := &KWDBTSStatementDescription{Name: name, TableName: tableName}

	var parseErr error

readloop:
	for {
		msg, err := pgConn.receiveMessage()
		if err != nil {
			pgConn.asyncClose()
			return nil, normalizeTimeoutError(ctx, err)
		}

		switch msg := msg.(type) {
		case *pgproto3.ParameterDescription:
			psd.ParamOIDs = append(psd.ParamOIDs[:0], msg.ParameterOIDs...)
		case *pgproto3.ErrorResponse:
			parseErr = ErrorResponseToPgError(msg)
		case *pgproto3.ReadyForQuery:
			break readloop
		case *pgproto3.ParameterDescriptionEx:
			psd.ParamOIDs = append(psd.ParamOIDs[:0], msg.ParameterOIDs...)
			psd.TagIndex = msg.TagIndex
			psd.PtagIDs = append(psd.PtagIDs[:0], msg.PtagIDs...)
			psd.StorageLen = append(psd.StorageLen[:0], msg.StorageLen...)
		}
	}

	if parseErr != nil {
		return nil, parseErr
	}
	return psd, nil
}

func (pgConn *PgConn) ExecPreparedKWDBTS(ctx context.Context, stmtName string, sd *KWDBTSStatementDescription, args [][]byte, colCountPerRow int) *ResultReader {
	result := pgConn.execExtendedPrefix(ctx, args)
	if result.closed {
		return result
	}

	payloads, err := buildKWDBTSPayloads(pgConn, sd, args, colCountPerRow)
	if err != nil {
		result.concludeCommand(CommandTag{}, err)
		pgConn.contextWatcher.Unwatch()
		result.closed = true
		pgConn.unlock()
		return result
	}
	defer releaseKWDBTSPayloads(pgConn, payloads)

	pgConn.frontend.SendBindEx(&pgproto3.BindEx{PreparedStatement: stmtName, PtagToPayload: payloads})
	pgConn.frontend.SendSync(&pgproto3.Sync{})

	err = pgConn.frontend.Flush()
	if err != nil {
		pgConn.asyncClose()
		result.concludeCommand(CommandTag{}, err)
		pgConn.contextWatcher.Unwatch()
		result.closed = true
		pgConn.unlock()
		return result
	}

	result.readUntilRowDescription()

	return result
}

func buildKWDBTSPayloads(pgConn *PgConn, sd *KWDBTSStatementDescription, args [][]byte, colCountPerRow int) (map[string]*pgproto3.PayloadBuffer, error) {
	if sd == nil {
		return nil, fmt.Errorf("KWDB TS statement description is nil")
	}
	if colCountPerRow <= 0 {
		return nil, fmt.Errorf("KWDB TS column count per row must be positive")
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("KWDB TS execution requires at least one row")
	}
	if len(args)%colCountPerRow != 0 {
		return nil, fmt.Errorf("KWDB TS execution row width mismatch: len(args)=%d colsPerRow=%d", len(args), colCountPerRow)
	}
	if len(sd.ParamOIDs) == 0 || len(sd.StorageLen) == 0 {
		return nil, fmt.Errorf("KWDB TS statement description is incomplete")
	}
	if len(sd.ParamOIDs) != len(sd.StorageLen) {
		return nil, fmt.Errorf("KWDB TS statement description storage mismatch: oids=%d storage=%d", len(sd.ParamOIDs), len(sd.StorageLen))
	}
	if sd.TagIndex < 0 || int(sd.TagIndex) > len(sd.ParamOIDs) {
		return nil, fmt.Errorf("KWDB TS statement description tag index is invalid: %d", sd.TagIndex)
	}
	for _, ptag := range sd.PtagIDs {
		if int(ptag) >= colCountPerRow {
			return nil, fmt.Errorf("KWDB TS primary tag index %d exceeds row width %d", ptag, colCountPerRow)
		}
	}

	payloads := make(map[string]*pgproto3.PayloadBuffer)
	var keyBuilder strings.Builder
	keyBuilder.Grow(64)

	maxRowlen := pgproto3.HeadSize + len(sd.StorageLen)
	for _, stlen := range sd.StorageLen {
		maxRowlen += int(stlen)
	}

	for pos := 0; pos < len(args); pos += colCountPerRow {
		row := args[pos : pos+colCountPerRow]
		key, err := buildKWDBTSGroupKey(&keyBuilder, row, sd.PtagIDs)
		if err != nil {
			releaseKWDBTSPayloads(pgConn, payloads)
			return nil, err
		}

		payload := payloads[key]
		if payload == nil {
			payload = pgConn.pool.Get().(*pgproto3.PayloadBuffer)
			payloads[key] = payload
		}

		if available := payload.Cap - payload.Tail; available < maxRowlen {
			growBy := maxRowlen - available
			if growBy < 4096 {
				growBy = 4096
			}
			if err := payload.Extend(growBy); err != nil {
				releaseKWDBTSPayloads(pgConn, payloads)
				return nil, err
			}
		}

		if err := payload.FillOneRow(row, sd.ParamOIDs, sd.PtagIDs, sd.TagIndex, sd.StorageLen, 0); err != nil {
			releaseKWDBTSPayloads(pgConn, payloads)
			return nil, err
		}
		payload.RowNum++
	}

	for _, payload := range payloads {
		if err := payload.WriteRowNum(); err != nil {
			releaseKWDBTSPayloads(pgConn, payloads)
			return nil, err
		}
	}

	return payloads, nil
}

func releaseKWDBTSPayloads(pgConn *PgConn, payloads map[string]*pgproto3.PayloadBuffer) {
	for _, payload := range payloads {
		_ = payload.Reset()
		pgConn.pool.Put(payload)
	}
}

func buildKWDBTSGroupKey(builder *strings.Builder, row [][]byte, ptagIDs []uint16) (string, error) {
	builder.Reset()
	for _, ptag := range ptagIDs {
		if int(ptag) >= len(row) || row[ptag] == nil {
			return "", fmt.Errorf("KWDB TS missing primary tag value at index %d", ptag)
		}
		builder.WriteString(strconv.Itoa(len(row[ptag])))
		builder.WriteByte(':')
		_, _ = builder.Write(row[ptag])
		builder.WriteByte('|')
	}
	return builder.String(), nil
}
