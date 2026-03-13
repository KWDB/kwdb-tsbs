package pgproto3

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

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
		return errors.New("extend memory failed")
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
	if pd.Data == nil || len(pd.Data) <= RowNumOffset || pd.HeadTail+4 > len(pd.Data) {
		return fmt.Errorf("payload buffer header is incomplete")
	}

	binary.LittleEndian.PutUint32(pd.Data[RowNumOffset:], pd.RowNum)
	binary.LittleEndian.PutUint32(pd.Data[pd.HeadTail:], uint32(pd.Tail-pd.HeadTail-4))
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

// FillColData encodes one KWDB TS column payload cell.
func FillColData(oid uint32, val []byte, dst []byte, storelen uint32, dstVarStart []byte,
	offsetVar uint16, isPtag bool) (int, int, error) {
	switch oid {
	case pgtype.Int8OID:
		if len(val) < 8 || len(dst) < 8 {
			return 0, 0, fmt.Errorf("int8 payload is too short")
		}
		binary.LittleEndian.PutUint64(dst, binary.BigEndian.Uint64(val))
		return 8, 0, nil
	case pgtype.TimestamptzOID:
		if len(val) < 8 || len(dst) < 8 {
			return 0, 0, fmt.Errorf("timestamptz payload is too short")
		}
		i := binary.BigEndian.Uint64(val)
		tm := PgBinaryToTime(int64(i))
		tum := tm.Unix()*1000 + int64(tm.Nanosecond()/1_000_000)
		binary.LittleEndian.PutUint64(dst, uint64(tum))
		return 8, 0, nil
	case pgtype.Int4OID:
		if len(val) < 8 || len(dst) < 4 {
			return 0, 0, fmt.Errorf("int4 payload is too short")
		}
		num := binary.BigEndian.Uint64(val)
		if num > uint64(math.MaxInt32) || int64(num) < int64(math.MinInt32) {
			return 0, 0, strconv.ErrRange
		}
		binary.LittleEndian.PutUint32(dst, uint32(num))
		return 4, 0, nil
	case pgtype.BPCharOID:
		if len(dst) < int(storelen) {
			return 0, 0, fmt.Errorf("bpchar payload destination is too short")
		}
		copy(dst[:storelen], val)
		return int(storelen), 0, nil
	case pgtype.VarcharOID:
		if len(val) > int(storelen) {
			return 0, 0, strconv.ErrRange
		}
		if isPtag {
			if len(dst) < int(storelen) {
				return 0, 0, fmt.Errorf("varchar primary tag destination is too short")
			}
			copy(dst[:storelen], val)
			return int(storelen), 0, nil
		}
		if len(dst) < VarColumnSize {
			return 0, 0, fmt.Errorf("varchar payload destination is too short")
		}
		currentVarPos := offsetVar
		requiredVar := int(currentVarPos) + VarDataLenSize + len(val) + 1
		if len(dstVarStart) < requiredVar {
			return 0, 0, fmt.Errorf("varchar varlen payload destination is too short")
		}
		binary.LittleEndian.PutUint64(dst, uint64(currentVarPos))
		binary.LittleEndian.PutUint16(dstVarStart[currentVarPos:], uint16(len(val)+1))
		currentVarPos += VarDataLenSize
		copy(dstVarStart[currentVarPos:], val)
		return VarColumnSize, int(offsetVar) + len(val) + VarDataLenSize + 1, nil
	default:
		return 0, 0, fmt.Errorf("unsupported KWDB TS OID %d", oid)
	}
}

func CalcVarPosHead(ptagDataLen int, posBaseHead int, idxTag int,
	storageLen []uint32) (int, int) {
	posVarStart := posBaseHead
	posVarStart += PTagLenSize
	posVarStart += ptagDataLen
	posVarStart += AllTagLenSize
	posTagStart := posVarStart
	posVarStart += int((len(storageLen)-idxTag)/8) + 1
	for i := idxTag; i < len(storageLen); i++ {
		posVarStart += int(storageLen[i])
	}
	return posTagStart, posVarStart
}

func computeRowSize(paramOIDs []uint32, storageLen []uint32) (int, error) {
	rowSize := 0
	for i, oid := range paramOIDs {
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
			rowSize += int(storageLen[i])
		case pgtype.VarcharOID:
			rowSize += VarColumnSize
		default:
			return 0, fmt.Errorf("unsupported KWDB TS OID %d", oid)
		}
	}
	return rowSize, nil
}

const (
	OnlyData       = 1
	OnlyTag        = 2
	BothTagAndData = 0
)

func (pd *PayloadBuffer) FillOneRow(args [][]byte, paramOIDs []uint32,
	ptagIDs []uint16, tagIndex int16, storageLen []uint32, rowNum uint32) error {
	var (
		err         error
		lenCol      int
		pos         int
		usedVarLen  int
		usingVarLen int
	)

	if pd.HeadTail == 0 {
		pos = HeadSize
		ptaglen := 0
		ptagDataStart := pos + PTagLenSize
		for _, pIdx := range ptagIDs {
			if int(pIdx) >= len(args) {
				return fmt.Errorf("missing primary tag argument at index %d", pIdx)
			}
			ptaglen += int(storageLen[pIdx])
			tagLen, _, fillErr := FillColData(paramOIDs[pIdx], args[pIdx], pd.Data[ptagDataStart:],
				storageLen[pIdx], nil, 0, true)
			if fillErr != nil {
				return fillErr
			}
			ptagDataStart += tagLen
		}

		taglen := 0
		posTagStart, posVar := CalcVarPosHead(ptaglen, pos, int(tagIndex), storageLen)
		binary.LittleEndian.PutUint16(pd.Data[pos:], uint16(ptaglen))

		tagLenPos := posTagStart - AllTagLenSize
		allColCount := len(storageLen)
		pos = posTagStart + int((allColCount-int(tagIndex))/8) + 1
		taglen += ((allColCount - int(tagIndex)) / 8) + 1
		payloadFlag := BothTagAndData
		for i := int(tagIndex); i < len(storageLen); i++ {
			if (i+1) > len(args) || args[i] == nil {
				pd.Data[posTagStart+((i-int(tagIndex))/8)] |= 1 << ((i - int(tagIndex)) % 8)
				pos += int(storageLen[i])
				taglen += int(storageLen[i])
				continue
			}
			payloadFlag = BothTagAndData
			lenCol, usingVarLen, err = FillColData(paramOIDs[i], args[i], pd.Data[pos:],
				storageLen[i], pd.Data[posVar:], uint16(usedVarLen), false)
			if err != nil {
				return err
			}
			if usingVarLen > 0 {
				usedVarLen += usingVarLen
			}
			pos += lenCol
			taglen += lenCol
		}
		pd.Data[RowNumOffset+RowNumSize] = byte(payloadFlag)
		binary.LittleEndian.PutUint32(pd.Data[tagLenPos:], uint32(taglen))
		pd.HeadTail = posVar + usedVarLen
		pd.Tail = pd.HeadTail + DataLenSize
	}

	rowStart := pd.Tail + DataLenSize
	usedVarLen = 0
	rowDataStart := rowStart + int(tagIndex/8) + 1
	lenTuple, err := computeRowSize(paramOIDs[:tagIndex], storageLen)
	if err != nil {
		return err
	}
	posRowVarStart := lenTuple + rowDataStart
	pos = rowDataStart
	for c := 0; c < int(tagIndex); c++ {
		if c >= len(args) {
			return fmt.Errorf("missing row data argument at index %d", c)
		}
		lenCol, usingVarLen, err = FillColData(paramOIDs[c], args[c],
			pd.Data[pos:], storageLen[c], pd.Data[posRowVarStart:], uint16(usedVarLen), false)
		if err != nil {
			return err
		}
		usedVarLen += usingVarLen
		pos += lenCol
	}

	rowLen := usedVarLen + (posRowVarStart - rowStart)
	binary.LittleEndian.PutUint32(pd.Data[pd.Tail:], uint32(rowLen))
	pd.Tail = posRowVarStart + usedVarLen
	_ = rowNum
	return nil
}
