package pgproto3

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

type KwDataRowBatch struct {
	RowNum          int32
	ColNum          int16
	RowSize         int32
	StorageLen      []int32
	ColBlockOffset  []int32
	Capacity        int32
	CompressionType int16
	Payload         []byte // raw data region (may be compressed per your protocol)

	Values         [][]byte
	ColOIDs        []uint32
	ValuesTextRows [][][]byte

	decompressor Decompressor
}
type Decompressor interface {
	Decompress(compressed, uncompressed []byte) error
}

func (*KwDataRowBatch) Backend() {}

// Decode parses the message body (without the type byte and length).
func (m *KwDataRowBatch) Decode(src []byte) error {

	if len(src) < 4+2+4 {
		return fmt.Errorf("KwDataRowBatch: invalid body length %d (<10)", len(src))
	}
	rp := 0

	m.RowNum = int32(binary.BigEndian.Uint32(src[rp:]))
	rp += 4

	m.ColNum = int16(binary.BigEndian.Uint16(src[rp:]))
	rp += 2

	m.RowSize = int32(binary.BigEndian.Uint32(src[rp:]))
	rp += 4

	rowNum := int(m.RowNum)
	colNum := int(m.ColNum)
	if rowNum < 0 {
		return fmt.Errorf("KwDataRowBatch: invalid rowNum %d", m.RowNum)
	}
	if colNum <= 0 || colNum > 4096 {
		return fmt.Errorf("KwDataRowBatch: invalid colNum %d", m.ColNum)
	}

	needMeta := colNum * (4 + 4)
	if rp+needMeta+4+2 > len(src) {
		return fmt.Errorf("KwDataRowBatch: body too short for meta (len=%d rp=%d)", len(src), rp)
	}

	if cap(m.StorageLen) < colNum {
		m.StorageLen = make([]int32, colNum)
		m.ColBlockOffset = make([]int32, colNum)
	} else {
		m.StorageLen = m.StorageLen[:colNum]
		m.ColBlockOffset = m.ColBlockOffset[:colNum]
	}

	// diff: (storageLen, colBlockOffset) per column, interleaved
	for i := 0; i < colNum; i++ {
		m.StorageLen[i] = int32(binary.BigEndian.Uint32(src[rp:]))
		rp += 4
		m.ColBlockOffset[i] = int32(binary.BigEndian.Uint32(src[rp:]))
		rp += 4
	}

	m.Capacity = int32(binary.BigEndian.Uint32(src[rp:]))
	rp += 4

	m.CompressionType = int16(binary.BigEndian.Uint16(src[rp:]))
	rp += 2

	// payload
	m.Payload = src[rp:]

	if m.CompressionType == 2 {
		rowNum := int(m.RowNum)
		colNum := int(m.ColNum)

		if len(m.ColOIDs) != colNum {
			return fmt.Errorf("KwDataRowBatch: ColOIDs missing: have %d want %d", len(m.ColOIDs), colNum)
		}

		payload := m.Payload
		m.ValuesTextRows = make([][][]byte, rowNum)
		for r := 0; r < rowNum; r++ {
			m.ValuesTextRows[r] = make([][]byte, colNum)
		}

		for c := 0; c < colNum; c++ {
			sl := int(m.StorageLen[c])
			off := int(m.ColBlockOffset[c])
			needEnd := off + rowNum*sl
			if sl < 0 || off < 0 || needEnd > len(payload) {
				return fmt.Errorf("KwDataRowBatch: payload range invalid col=%d sl=%d off=%d needEnd=%d payload=%d",
					c, sl, off, needEnd, len(payload))
			}

			oid := m.ColOIDs[c]
			for r := 0; r < rowNum; r++ {
				start := off + r*sl
				end := start + sl
				raw := payload[start:end]

				txt, err := kwCellToText(oid, raw)
				if err != nil {
					return err
				}
				m.ValuesTextRows[r][c] = txt
			}
		}
	} else if m.CompressionType == 4 || m.CompressionType == 3 {

		m.ValuesTextRows = make([][][]byte, rowNum)
		for r := 0; r < rowNum; r++ {
			m.ValuesTextRows[r] = make([][]byte, colNum)
		}
		payload := m.Payload
		offset := 0

		for col := 0; col < colNum; col++ {
			if len(payload) < 8 {
				return fmt.Errorf("KwDataRowBatch: payload too short for compression header at col=%d", col)
			}

			uncompressedSize := int32(binary.BigEndian.Uint32(payload[0:4]))
			compressedSize := int32(binary.BigEndian.Uint32(payload[4:8]))
			payload = payload[8:]

			if int(compressedSize) > len(payload) {
				return fmt.Errorf("KwDataRowBatch: compressed data too short col=%d, need=%d, have=%d",
					col, compressedSize, len(payload))
			}

			compressedData := payload[:compressedSize]
			payload = payload[compressedSize:]

			uncompressedBuffer := make([]byte, uncompressedSize)
			if compressedSize > 0 {
				err := m.decompressBlock(compressedData, uncompressedBuffer)
				if err != nil {
					return fmt.Errorf("KwDataRowBatch: decompress failed col=%d: %v", col, err)
				}
			} else {
				copy(uncompressedBuffer, compressedData)
			}

			storageLen := int(m.StorageLen[col])
			colBlockOffset := int(m.ColBlockOffset[col])

			dataStart := colBlockOffset - offset
			if dataStart < 0 || dataStart >= len(uncompressedBuffer) {
				return fmt.Errorf("KwDataRowBatch: invalid data start col=%d offset=%d start=%d buffer=%d",
					col, offset, dataStart, len(uncompressedBuffer))
			}

			offset += int(uncompressedSize)
			dataSlice := uncompressedBuffer[dataStart:]

			for row := 0; row < rowNum; row++ {
				if len(dataSlice) < storageLen {
					return fmt.Errorf("KwDataRowBatch: insufficient data for row=%d col=%d need=%d have=%d",
						row, col, storageLen, len(dataSlice))
				}

				rowData := dataSlice[:storageLen]
				dataSlice = dataSlice[storageLen:]

				oid := m.ColOIDs[col]
				txt, err := kwCellToText(oid, rowData)
				if err != nil {
					return fmt.Errorf("KwDataRowBatch: cell conversion failed row=%d col=%d: %v",
						row, col, err)
				}

				m.ValuesTextRows[row][col] = txt
			}
		}

	}
	return nil
}

// Encode builds the full wire message (type + length + body) into dst.
// This signature matches your BackendMessage interface.
func (m *KwDataRowBatch) Encode(dst []byte) []byte {
	return nil
}

func kwCellToText(oid uint32, cell []byte) ([]byte, error) {
	switch oid {
	case 1184, 1114, 1082: // timestamptz/timestamp/date
		if len(cell) < 8 {
			return nil, fmt.Errorf("ts cell too short %d", len(cell))
		}
		//v := int64(binary.LittleEndian.Uint64(cell[:8]))
		//t := time.Unix(0, v*int64(time.Millisecond)).UTC()
		return cell[:8], nil

	case 701: // float8
		if len(cell) < 8 {
			return nil, fmt.Errorf("float8 cell too short %d", len(cell))
		}
		//f := math.Float64frombits(binary.LittleEndian.Uint64(cell[:8]))
		return cell[:8], nil

	case 700: // float4
		if len(cell) < 4 {
			return nil, fmt.Errorf("float4 cell too short %d", len(cell))
		}
		//f := math.Float32frombits(binary.LittleEndian.Uint32(cell[:4]))
		return cell[:4], nil

	case 20: // int8
		if len(cell) < 8 {
			return nil, fmt.Errorf("int8 cell too short %d", len(cell))
		}
		//v := int64(binary.LittleEndian.Uint64(cell[:8]))
		return cell[:8], nil

	case 23: // int4
		if len(cell) < 4 {
			return nil, fmt.Errorf("int4 cell too short %d", len(cell))
		}
		//v := int64(int32(binary.LittleEndian.Uint32(cell[:4])))
		return cell[:4], nil

	case 25, 1043, 1042: // text/varchar/bpchar
		if len(cell) < 2 {
			return []byte{}, nil
		}
		b := cell[2:]
		for i, x := range b {
			if x == 0 {
				b = b[:i]
				break
			}
		}
		return b, nil

	default:
		return []byte(hex.EncodeToString(cell)), nil
	}
}

// decompressBlock uncompress the data block
func (m *KwDataRowBatch) decompressBlock(compressed, uncompressed []byte) error {
	//  first attempt to extract with snappy
	_, err := snappy.Decode(uncompressed, compressed)
	if err != nil {
		// if snappy fails, try lz4
		_, err = lz4.UncompressBlock(compressed, uncompressed)
		if err != nil {
			return fmt.Errorf("decompress failed with both snappy and lz4: %v", err)
		}
	}

	return nil
}
