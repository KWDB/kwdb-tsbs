package pgproto3

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/internal/pgio"
	"github.com/jackc/pgx/v5/pgtype"
	"math"
	"strconv"
	"time"
)

type Bind struct {
	DestinationPortal    string
	PreparedStatement    string
	ParameterFormatCodes []int16
	Parameters           [][]byte
	ResultFormatCodes    []int16
}

// Frontend identifies this message as sendable by a PostgreSQL frontend.
func (*Bind) Frontend() {}

// Decode decodes src into dst. src must contain the complete message with the exception of the initial 1 byte message
// type identifier and 4 byte message length.
func (dst *Bind) Decode(src []byte) error {
	*dst = Bind{}

	idx := bytes.IndexByte(src, 0)
	if idx < 0 {
		return &invalidMessageFormatErr{messageType: "Bind"}
	}
	dst.DestinationPortal = string(src[:idx])
	rp := idx + 1

	idx = bytes.IndexByte(src[rp:], 0)
	if idx < 0 {
		return &invalidMessageFormatErr{messageType: "Bind"}
	}
	dst.PreparedStatement = string(src[rp : rp+idx])
	rp += idx + 1

	if len(src[rp:]) < 2 {
		return &invalidMessageFormatErr{messageType: "Bind"}
	}
	parameterFormatCodeCount := int(binary.BigEndian.Uint16(src[rp:]))
	rp += 2

	if parameterFormatCodeCount > 0 {
		dst.ParameterFormatCodes = make([]int16, parameterFormatCodeCount)

		if len(src[rp:]) < len(dst.ParameterFormatCodes)*2 {
			return &invalidMessageFormatErr{messageType: "Bind"}
		}
		for i := 0; i < parameterFormatCodeCount; i++ {
			dst.ParameterFormatCodes[i] = int16(binary.BigEndian.Uint16(src[rp:]))
			rp += 2
		}
	}

	if len(src[rp:]) < 2 {
		return &invalidMessageFormatErr{messageType: "Bind"}
	}
	parameterCount := int(binary.BigEndian.Uint16(src[rp:]))
	rp += 2

	if parameterCount > 0 {
		dst.Parameters = make([][]byte, parameterCount)

		for i := 0; i < parameterCount; i++ {
			if len(src[rp:]) < 4 {
				return &invalidMessageFormatErr{messageType: "Bind"}
			}

			msgSize := int(int32(binary.BigEndian.Uint32(src[rp:])))
			rp += 4

			// null
			if msgSize == -1 {
				continue
			}

			if len(src[rp:]) < msgSize {
				return &invalidMessageFormatErr{messageType: "Bind"}
			}

			dst.Parameters[i] = src[rp : rp+msgSize]
			rp += msgSize
		}
	}

	if len(src[rp:]) < 2 {
		return &invalidMessageFormatErr{messageType: "Bind"}
	}
	resultFormatCodeCount := int(binary.BigEndian.Uint16(src[rp:]))
	rp += 2

	dst.ResultFormatCodes = make([]int16, resultFormatCodeCount)
	if len(src[rp:]) < len(dst.ResultFormatCodes)*2 {
		return &invalidMessageFormatErr{messageType: "Bind"}
	}
	for i := 0; i < resultFormatCodeCount; i++ {
		dst.ResultFormatCodes[i] = int16(binary.BigEndian.Uint16(src[rp:]))
		rp += 2
	}

	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *Bind) Encode(dst []byte) []byte {
	dst = append(dst, 'B')
	sp := len(dst)
	dst = pgio.AppendInt32(dst, -1)

	dst = append(dst, src.DestinationPortal...)
	dst = append(dst, 0)
	dst = append(dst, src.PreparedStatement...)
	dst = append(dst, 0)

	dst = pgio.AppendUint16(dst, uint16(len(src.ParameterFormatCodes)))
	for _, fc := range src.ParameterFormatCodes {
		dst = pgio.AppendInt16(dst, fc)
	}

	dst = pgio.AppendUint16(dst, uint16(len(src.Parameters)))
	for _, p := range src.Parameters {
		if p == nil {
			dst = pgio.AppendInt32(dst, -1)
			continue
		}

		dst = pgio.AppendInt32(dst, int32(len(p)))
		dst = append(dst, p...)
	}

	dst = pgio.AppendUint16(dst, uint16(len(src.ResultFormatCodes)))
	for _, fc := range src.ResultFormatCodes {
		dst = pgio.AppendInt16(dst, fc)
	}

	pgio.SetInt32(dst[sp:], int32(len(dst[sp:])))

	return dst
}

// MarshalJSON implements encoding/json.Marshaler.
func (src Bind) MarshalJSON() ([]byte, error) {
	formattedParameters := make([]map[string]string, len(src.Parameters))
	for i, p := range src.Parameters {
		if p == nil {
			continue
		}

		textFormat := true
		if len(src.ParameterFormatCodes) == 1 {
			textFormat = src.ParameterFormatCodes[0] == 0
		} else if len(src.ParameterFormatCodes) > 1 {
			textFormat = src.ParameterFormatCodes[i] == 0
		}

		if textFormat {
			formattedParameters[i] = map[string]string{"text": string(p)}
		} else {
			formattedParameters[i] = map[string]string{"binary": hex.EncodeToString(p)}
		}
	}

	return json.Marshal(struct {
		Type                 string
		DestinationPortal    string
		PreparedStatement    string
		ParameterFormatCodes []int16
		Parameters           []map[string]string
		ResultFormatCodes    []int16
	}{
		Type:                 "Bind",
		DestinationPortal:    src.DestinationPortal,
		PreparedStatement:    src.PreparedStatement,
		ParameterFormatCodes: src.ParameterFormatCodes,
		Parameters:           formattedParameters,
		ResultFormatCodes:    src.ResultFormatCodes,
	})
}

// UnmarshalJSON implements encoding/json.Unmarshaler.
func (dst *Bind) UnmarshalJSON(data []byte) error {
	// Ignore null, like in the main JSON package.
	if string(data) == "null" {
		return nil
	}

	var msg struct {
		DestinationPortal    string
		PreparedStatement    string
		ParameterFormatCodes []int16
		Parameters           []map[string]string
		ResultFormatCodes    []int16
	}
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return err
	}
	dst.DestinationPortal = msg.DestinationPortal
	dst.PreparedStatement = msg.PreparedStatement
	dst.ParameterFormatCodes = msg.ParameterFormatCodes
	dst.Parameters = make([][]byte, len(msg.Parameters))
	dst.ResultFormatCodes = msg.ResultFormatCodes
	for n, parameter := range msg.Parameters {
		dst.Parameters[n], err = getValueFromJSON(parameter)
		if err != nil {
			return fmt.Errorf("cannot get param %d: %w", n, err)
		}
	}
	return nil
}

const (
	// OsnIDOffset offset of osn_id in the payload header
	OsnIDOffset = 0
	// OsnIDSize length of osn_id in the payload header (Only 8 bytes have been used).
	OsnIDSize = 16
	// RangeGroupIDOffset offset of range_group_id in the payload header
	RangeGroupIDOffset = 16
	// RangeGroupIDSize length of range_group_id in the payload header
	RangeGroupIDSize = 2
	// PayloadVersionOffset offset of payload_version in the payload header
	PayloadVersionOffset = 18
	// PayloadVersionSize length of payload_version in the payload header
	PayloadVersionSize = 4
	// DBIDOffset offset of db_id in the payload header
	DBIDOffset = 22
	// DBIDSize length of db_id in the payload header
	DBIDSize = 4
	// TableIDOffset offset of table_id in the payload header
	TableIDOffset = 26
	// TableIDSize length of table_id in the payload header
	TableIDSize = 8
	// TSVersionOffset offset of ts_version in the payload header
	TSVersionOffset = 34
	// TSVersionSize length of ts_version in the payload header
	TSVersionSize = 4
	// RowNumOffset offset of row_num in the payload header
	RowNumOffset = 38
	// RowNumSize length of row_num in the payload header
	RowNumSize = 4
	// RowTypeOffset offset of row_type in the payload header
	RowTypeOffset = 42
	// RowTypeSize length of row_type in the payload header
	RowTypeSize = 1
	// HeadSize is the payload fixed header length of insert ts table
	HeadSize = RowTypeOffset + RowTypeSize
	// PTagLenSize length of primary tag
	PTagLenSize = 2
	// AllTagLenSize length of ordinary tag
	AllTagLenSize = 4
	// DataLenSize length of datalen size
	DataLenSize = 4
	// VarDataLenSize length of not fixed datalen
	VarDataLenSize = 2
	// VarColumnSize is the fixed length memory taken by var-length data type
	VarColumnSize = 8
)

type PayloadBuffer struct {
	Tail     int
	Data     []byte
	Cap      int
	HeadTail int
	rowTail  int
	RowNum   uint32
}

func (pd *PayloadBuffer) Extend(size int) error {
	if size <= 0 {
		return nil
	}
	newSize := pd.Cap + size
	dataNew := make([]byte, newSize)
	if dataNew == nil {
		return errors.New("extend memory failed.")
	}

	if pd.Data == nil {
		pd.Tail = 0
		pd.Data = dataNew
	} else {
		useData := pd.Data[:pd.Tail]
		copy(dataNew, useData)
		pd.Data = dataNew
	}

	pd.Cap = newSize
	return nil
}

func (pd *PayloadBuffer) WriteRowNum() error {
	if pd.Data != nil && len(pd.Data) > RowNumOffset {
		binary.LittleEndian.PutUint32(pd.Data[RowNumOffset:], pd.RowNum)
		binary.LittleEndian.PutUint32(pd.Data[pd.HeadTail:], uint32(pd.Tail-pd.HeadTail-4)) // data len
	}

	return nil
}

func (pd *PayloadBuffer) Reset() error {
	pd.Tail = 0
	pd.HeadTail = 0
	pd.rowTail = 0
	pd.RowNum = 0

	return nil
}

const (
	minTimeDuration time.Duration = -1 << 63
	maxTimeDuration time.Duration = 1<<63 - 1
)

func AddMicros(t time.Time, d int64) time.Time {
	negMult := time.Duration(1)
	if d < 0 {
		negMult = -1
		d = -d
	}
	const maxMicroDur = int64(maxTimeDuration / time.Microsecond)
	for d > maxMicroDur {
		const maxWholeNanoDur = time.Duration(maxMicroDur) * time.Microsecond
		t = t.Add(negMult * maxWholeNanoDur)
		d -= maxMicroDur
	}
	return t.Add(negMult * time.Duration(d) * time.Microsecond)
}

func PgBinaryToTime(i int64) time.Time {
	return AddMicros(PGEpochJDate, i)
}

var (
	// PGEpochJDate represents the pg epoch.
	PGEpochJDate = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
)

// fill tag and metric data
func FillColData(oid uint32, val []byte, dst []byte, storelen uint32, dstVarStart []byte,
	offsetVar uint16, isPtag bool) (int, int, error) {
	switch oid { // not all support
	case pgtype.Int8OID:
		// val, err := strconv.Atoi(string(val))
		//if err != nil {
		//	return -1, -1, err
		//}
		binary.LittleEndian.PutUint64(dst, binary.BigEndian.Uint64(val))
		return 8, 0, nil
	case pgtype.TimestamptzOID:
		//loc := time.FixedZone("UTC", 8*3600) // UTC+8
		// This will look for the name CEST in the Europe/Berlin time zone.
		//const longForm = "2006-01-02 15:04:05"
		//t, _ := time.ParseInLocation(longForm, string(val), loc)
		//milliseconds := t.UnixNano() / int64(time.Millisecond)
		i := binary.BigEndian.Uint64(val)
		tm := PgBinaryToTime(int64(i))
		nanosecond := tm.Nanosecond()
		second := tm.Unix()
		tum := second*1000 + int64(nanosecond/1000000)
		binary.LittleEndian.PutUint64(dst, uint64(tum))
		return 8, 0, nil
	case pgtype.Int4OID:
		num := binary.BigEndian.Uint64(val)
		if num > uint64(math.MaxInt32) || int64(num) < int64(math.MinInt32) {
			return -1, -1, strconv.ErrRange
		}
		binary.LittleEndian.PutUint32(dst, uint32(num))
		return 4, 0, nil
	case pgtype.BPCharOID:
		copy(dst, val)
		return int(storelen), 0, nil
	case pgtype.VarcharOID:
		if len(val) > int(storelen) {
			return -1, 0, strconv.ErrRange
		}
		if isPtag {
			copy(dst, val)
			return int(storelen), 0, nil
		}
		currentVarPos := offsetVar
		binary.LittleEndian.PutUint64(dst, uint64(currentVarPos))
		// var part
		binary.LittleEndian.PutUint16(dstVarStart[currentVarPos:], uint16(len(val)+1))
		currentVarPos += 2
		copy(dstVarStart[currentVarPos:], val)
		return 8, int(offsetVar) + len(val) + 2 + 1, nil // 8 real 4
	default:
		panic(0)
	}
}

func CalcVarPosHead(ptagDataLen int, posBaseHead int, idxTag int,
	StorageLen []uint32) (int, int) {
	posTagStart := 0
	posVarStart := 0
	posVarStart = posBaseHead
	posVarStart += 2
	posVarStart += ptagDataLen
	posVarStart += 4
	posTagStart = posVarStart
	// bitmap
	allColCount := len(StorageLen)
	posVarStart += int((allColCount-idxTag)/8) + 1
	for i := idxTag; i < len(StorageLen); i++ {
		posVarStart += int(StorageLen[i])
	}
	return posTagStart, posVarStart
}

// calc metric col row size
func computeRowSize(ParamOIDs []uint32, StorageLen []uint32) int {
	rowSize := 0
	for i, oid := range ParamOIDs {
		switch oid {
		case pgtype.Int8OID:
			rowSize += 8
		case pgtype.Int2OID:
			rowSize += 2
		case pgtype.Int4OID:
			rowSize += 4
		case pgtype.TimestamptzOID:
			rowSize += 8
		case pgtype.BPCharOID:
			rowSize += int(StorageLen[i])
		case pgtype.VarcharOID:
			rowSize += VarColumnSize
		default:
			panic(0)
		}
	}
	return rowSize
}

const (
	OnlyData       = 1
	OnlyTag        = 2
	BothTagAndData = 0
)

func (pd *PayloadBuffer) FillOneRow(args [][]byte, ParamOIDs []uint32,
	PtagIDs []uint16, TagIndex int16, StorageLen []uint32, rowNum uint32) error {
	var err error
	lenCol := 0
	pos := 0
	usedVarLen := 0
	usingVarLen := 0
	if pd.HeadTail == 0 {
		pos = HeadSize
		// add head
		// header(43)+ptaglen(2)+ptag+taglen(4)+tag
		// tag feild len with define coldes
		ptaglen := 0
		// write ptag value and len
		ptagDataStart := pos + PTagLenSize
		for _, pIdx := range PtagIDs {
			ptaglen += int(StorageLen[pIdx])
			tagLen, _, err := FillColData(ParamOIDs[pIdx], args[pIdx], pd.Data[ptagDataStart:],
				StorageLen[pIdx], nil, 0, true)
			if err != nil {
				return err
			}
			ptagDataStart += tagLen
		}

		taglen := 0

		// rowNum
		// binary.LittleEndian.PutUint32(pd.Data[RowNumOffset:], rowNum)
		posTagStart, posVar := CalcVarPosHead(ptaglen, pos, int(TagIndex), StorageLen)
		// ptag data
		binary.LittleEndian.PutUint16(pd.Data[pos:], uint16(ptaglen))
		pos += 2
		// copy(pd.Data[pos:], ptag)

		// tag data
		tagLenPos := posTagStart - 4
		allColCount := len(StorageLen)
		pos = posTagStart + int((allColCount-int(TagIndex))/8) + 1 // bitmap
		taglen += ((allColCount - int(TagIndex)) / 8) + 1
		payloadFlag := BothTagAndData
		for i := int(TagIndex); i < len(StorageLen); i++ {
			if (i+1) > len(args) || args[i] == nil {
				pd.Data[posTagStart+((i-int(TagIndex))/8)] |= 1 << ((i - int(TagIndex)) % 8)
				pos += int(StorageLen[int(i)])
				taglen += int(StorageLen[int(i)])
				continue
			}
			payloadFlag = BothTagAndData
			lenCol, usingVarLen, err = FillColData(ParamOIDs[i], args[i], pd.Data[pos:],
				StorageLen[i], pd.Data[posVar:], uint16(usedVarLen), false)
			if err != nil {
				return err
			}
			if usingVarLen > 0 {
				usedVarLen += usingVarLen
			}
			taglen += lenCol
		}
		pd.Data[RowNumOffset+4] = byte(payloadFlag)
		binary.LittleEndian.PutUint32(pd.Data[tagLenPos:], uint32(taglen))
		pd.HeadTail = posVar + usedVarLen
		pd.Tail = pd.HeadTail + 4 // data len
	}
	// data part
	rowStart := pd.Tail + 4 // row length
	usedVarLen = 0
	rowDataStart := rowStart + int(TagIndex/8) + 1 // bitmap
	lenTuple := computeRowSize(ParamOIDs[:TagIndex], StorageLen)
	posRowVarStart := lenTuple + rowDataStart
	pos = rowDataStart
	for c := 0; c < int(TagIndex); c++ {
		lenCol, usingVarLen, err = FillColData(ParamOIDs[c], args[c],
			pd.Data[pos:], StorageLen[c], pd.Data[posRowVarStart:], uint16(usedVarLen), false)
		if err != nil {
			return err
		}
		// lenTuple += lenCol
		usedVarLen += usingVarLen
		pos += lenCol
	}

	// write row len other this row invalid
	rowLen := usedVarLen + (posRowVarStart - rowStart) // var len and data len and bitmap
	binary.LittleEndian.PutUint32(pd.Data[pd.Tail:], uint32(rowLen))
	pd.Tail = posRowVarStart + usedVarLen
	return nil
}

type BindEx struct {
	DestinationPortal string
	PreparedStatement string
	PtagToPayload     map[string]*PayloadBuffer
}

// Frontend identifies this message as sendable by a PostgreSQL frontend.
func (*BindEx) Frontend() {}

// Decode decodes src into dst. src must contain the complete message with the exception of the initial 1 byte message
// type identifier and 4 byte message length.
func (dst *BindEx) Decode(src []byte) error {
	*dst = BindEx{}

	idx := bytes.IndexByte(src, 0)
	if idx < 0 {
		return &invalidMessageFormatErr{messageType: "BindEx"}
	}
	dst.DestinationPortal = string(src[:idx])
	rp := idx + 1

	idx = bytes.IndexByte(src[rp:], 0)
	if idx < 0 {
		return &invalidMessageFormatErr{messageType: "BindEx"}
	}
	dst.PreparedStatement = string(src[rp : rp+idx])
	rp += idx + 1

	if len(src[rp:]) < 2 {
		return &invalidMessageFormatErr{messageType: "BindEx"}
	}
	// payload count
	// all payload
	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *BindEx) Encode(dst []byte) []byte {
	dst = append(dst, 'W')
	sp := len(dst)
	dst = pgio.AppendInt32(dst, -1)

	dst = append(dst, src.DestinationPortal...)
	dst = append(dst, 0)
	dst = append(dst, src.PreparedStatement...)
	dst = append(dst, 0)

	// all payloads
	dst = pgio.AppendInt16(dst, int16(len(src.PtagToPayload))) // payload count

	for ptag, payload := range src.PtagToPayload {
		dst = append(dst, ptag...)
		dst = append(dst, 0)

		// data
		dst = pgio.AppendInt32(dst, int32(payload.Tail))
		dst = append(dst, payload.Data[:payload.Tail]...)
	}

	pgio.SetInt32(dst[sp:], int32(len(dst[sp:])))

	return dst
}

// MarshalJSON implements encoding/json.Marshaler.
func (src BindEx) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type              string
		DestinationPortal string
		PreparedStatement string
	}{
		Type:              "BindEx",
		DestinationPortal: src.DestinationPortal,
		PreparedStatement: src.PreparedStatement,
	})
}

// UnmarshalJSON implements encoding/json.Unmarshaler.
func (dst *BindEx) UnmarshalJSON(data []byte) error {

	return nil
}
