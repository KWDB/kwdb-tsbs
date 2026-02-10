package pgproto3

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

//
// The src argument (message body) has the following format (BigEndian):
//
// | Field             | Type    | Size (Bytes)    | Description                                      |
// |-------------------|---------|-----------------|--------------------------------------------------|
// | RowNum            | int32   | 4               | Number of rows in the batch                      |
// | ColNum            | int16   | 2               | Number of columns                                |
// | RowSize           | int32   | 4               | Size of a row                                    |
// | Column Metadata   | Array   | ColNum * 8      | Repeated for each column (StorageLen + Offset)   |
// | Capacity          | int32   | 4               | Batch capacity                                   |
// | CompressionType   | int16   | 2               | 2=Uncompressed, 3/4=Compressed                   |
// | Payload           | []byte  | Variable        | Data bytes (structure depends on CompressionType)|
//
// Column Metadata Structure (Repeated ColNum times):
// | Field             | Type    | Size (Bytes)    | Description                                      |
// |-------------------|---------|-----------------|--------------------------------------------------|
// | StorageLen        | int32   | 4               | Data length per cell for this column             |
// | ColBlockOffset    | int32   | 4               | Offset in Payload for this column's data         |
//
// Payload Structure (CompressionType == 3 or 4):
// Sequence of compressed blocks for each column.
//
// | Field             | Type    | Size (Bytes)    | Description                                      |
// |-------------------|---------|-----------------|--------------------------------------------------|
// | UncompressedSize  | int32   | 4               | Size of data after decompression                 |
// | CompressedSize    | int32   | 4               | Size of the following compressed data            |
// | CompressedData    | []byte  | CompressedSize  | The actual compressed data                       |
//
// Message Body Layout Diagram:
//
// +-----------------------------------------------------------------------+
// | RowNum (4 bytes)   | ColNum (2 bytes)     | RowSize (4 bytes)         |
// +-----------------------------------------------------------------------+
// |                    Column Metadata (ColNum * 8 bytes)                 |
// | +---------------------------+---------------------------+-----------+ |
// | | Col 0: StorageLen (4B)    | Col 0: BlockOffset (4B)   |           | |
// | +---------------------------+---------------------------+    ...    | |
// | | Col N: StorageLen (4B)    | Col N: BlockOffset (4B)   |           | |
// | +---------------------------+---------------------------+-----------+ |
// +-----------------------------------------------------------------------+
// | Capacity (4 bytes)          | CompressionType (2 bytes)               |
// +-----------------------------------------------------------------------+
// |                               Payload                                 |
// | +-------------------------------------------------------------------+ |
// | | If CompressionType == 2 (Uncompressed):                           | |
// | |   [Raw Data Bytes for Col 0, Row 0..M]                            | |
// | |   ...                                                             | |
// | |   [Raw Data Bytes for Col N, Row 0..M]                            | |
// | +-------------------------------------------------------------------+ |
// | | If CompressionType == 3 or 4 (Compressed):                        | |
// | |   +-----------------------------------------------------------+   | |
// | |   | Col 0 Block:                                              |   | |
// | |   |   UncompressedSize (4B) | CompressedSize (4B)             |   | |
// | |   |   [Compressed Data ... (CompressedSize bytes)]            |   | |
// | |   +-----------------------------------------------------------+   | |
// | |   ...                                                             | |
// | |   +-----------------------------------------------------------+   | |
// | |   | Col N Block:                                              |   | |
// | |   |   UncompressedSize (4B) | CompressedSize (4B)             |   | |
// | |   |   [Compressed Data ... (CompressedSize bytes)]            |   | |
// | |   +-----------------------------------------------------------+   | |
// | +-------------------------------------------------------------------+ |
// +-----------------------------------------------------------------------+

// DataRowBatch represents a batch of data rows in a column-oriented format.
const (
	sizeRowNum  = 4
	sizeColNum  = 2
	sizeRowSize = 4

	minMsgBodyLen = sizeRowNum + sizeColNum + sizeRowSize

	sizeStorageLen     = 4
	sizeColBlockOffset = 4
	sizeColumnMetadata = 8

	sizeCapacity        = 4
	sizeCompressionType = 2

	sizeUncompressed        = 4
	sizeCompressed          = 4
	sizeCompressedBlockHead = sizeUncompressed + sizeCompressed
)

// Compression type constants.
const (
	CompressionTypeNone   int16 = 2
	CompressionTypeSnappy int16 = 3
	CompressionTypeLZ4    int16 = 4
)

// Column count upper bound.
const maxColumns = 4096

// PostgreSQL OID constants for supported types.
const (
	oidTimestampTZ uint32 = 1184
	oidTimestamp   uint32 = 1114
	oidDate        uint32 = 1082
	oidFloat8      uint32 = 701
	oidFloat4      uint32 = 700
	oidInt8        uint32 = 20
	oidInt4        uint32 = 23
	oidText        uint32 = 25
	oidVarchar     uint32 = 1043
	oidBpchar      uint32 = 1042
)

// KwDataRowBatch represents a batch of data rows in a column-oriented format.
type KwDataRowBatch struct {
	RowNum          uint32
	ColNum          uint16
	RowSize         uint32
	StorageLen      []uint32
	ColBlockOffset  []uint32
	Capacity        uint32
	CompressionType int16
	Payload         []byte // raw data region (may be compressed per your protocol)

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
	rowNum, colNum, err := m.decodeMeta(src)
	if err != nil {
		return err
	}

	switch m.CompressionType {
	case CompressionTypeNone:
		return m.decodeUncompressed(rowNum, colNum)
	case CompressionTypeSnappy, CompressionTypeLZ4:
		return m.decodeCompressed(rowNum, colNum)
	default:
		return fmt.Errorf("KwDataRowBatch: unknown compression type %d", m.CompressionType)
	}
}

// Decode parses the message body (without the type byte and length).
func (m *KwDataRowBatch) decodeMeta(src []byte) (rowNum, colNum int, err error) {
	msglen := len(src)

	if msglen < minMsgBodyLen {
		return 0, 0, fmt.Errorf("KwDataRowBatch: invalid body length %d (<10)", msglen)
	}

	m.RowNum = binary.BigEndian.Uint32(src)
	m.ColNum = binary.BigEndian.Uint16(src[sizeRowNum:])
	m.RowSize = binary.BigEndian.Uint32(src[sizeRowNum+sizeColNum:])

	if m.RowNum == 0 {
		return 0, 0, fmt.Errorf("KwDataRowBatch: invalid rowNum %d", m.RowNum)
	}

	if m.ColNum == 0 || m.ColNum > maxColumns {
		return 0, 0, fmt.Errorf("KwDataRowBatch: invalid colNum %d", m.ColNum)
	}

	rowNum = int(m.RowNum)
	colNum = int(m.ColNum)

	requiredMetadataLen := minMsgBodyLen + colNum*sizeColumnMetadata
	if requiredMetadataLen > msglen {
		return 0, 0, fmt.Errorf("KwDataRowBatch: body too short for %d meta (len=%d)", m.ColNum, msglen)
	}

	if cap(m.StorageLen) < colNum {
		m.StorageLen = make([]uint32, colNum)
		m.ColBlockOffset = make([]uint32, colNum)
	} else {
		m.StorageLen = m.StorageLen[:colNum]
		m.ColBlockOffset = m.ColBlockOffset[:colNum]
	}

	// Parse column metadata
	rp := minMsgBodyLen
	for i := 0; i < colNum; i++ {
		m.StorageLen[i] = binary.BigEndian.Uint32(src[rp:])
		m.ColBlockOffset[i] = binary.BigEndian.Uint32(src[rp+sizeStorageLen:])
		rp += sizeColumnMetadata
	}

	m.Capacity = binary.BigEndian.Uint32(src[rp:])
	rp += sizeCapacity

	m.CompressionType = int16(binary.BigEndian.Uint16(src[rp:]))
	rp += sizeCompressionType

	if len(m.ColOIDs) != colNum {
		return 0, 0, fmt.Errorf("KwDataRowBatch: ColOIDs missing: have %d want %d", len(m.ColOIDs), colNum)
	}

	if cap(m.ValuesTextRows) < rowNum {
		m.ValuesTextRows = make([][][]byte, rowNum)
		allRows := make([][]byte, rowNum*colNum)
		for r := 0; r < rowNum; r++ {
			m.ValuesTextRows[r] = allRows[r*colNum : (r+1)*colNum]
		}
	} else {
		m.ValuesTextRows = m.ValuesTextRows[:rowNum]
		if cap(m.ValuesTextRows[0]) < colNum {
			allRows := make([][]byte, rowNum*colNum)
			for r := 0; r < rowNum; r++ {
				m.ValuesTextRows[r] = allRows[r*colNum : (r+1)*colNum]
			}
		} else {
			for r := 0; r < rowNum; r++ {
				m.ValuesTextRows[r] = m.ValuesTextRows[r][:colNum]
			}
		}
	}

	m.Payload = src[rp:]
	return rowNum, colNum, nil
}

// decodeUncompressed handles uncompressed payload decoding
func (m *KwDataRowBatch) decodeUncompressed(rowNum, colNum int) error {
	payload := m.Payload
	lenPayload := len(payload)

	for c := 0; c < colNum; c++ {
		sl := int(m.StorageLen[c])
		off := int(m.ColBlockOffset[c])
		colEnd := off + rowNum*sl

		// Single bounds check for the entire column block
		if sl < 0 || off < 0 || colEnd > lenPayload {
			return fmt.Errorf("col=%d: invalid storageLen=%d offset=%d colEnd=%d lenPayload=%d", c, sl, off, colEnd, lenPayload)
		}

		// Slice to exact column range for BCE optimization
		colData := payload[off:colEnd]
		oid := m.ColOIDs[c]

		for r := 0; r < rowNum; r++ {
			start := r * sl
			end := start + sl
			raw := colData[start:end]

			txt, err := kwCellToText(oid, raw)
			if err != nil {
				return fmt.Errorf("col=%d row=%d: %w", c, r, err)
			}
			m.ValuesTextRows[r][c] = txt
		}
	}
	return nil
}

// decodeCompressed handles compressed payload decoding
func (m *KwDataRowBatch) decodeCompressed(rowNum, colNum int) error {
	payload := m.Payload

	// New format: one compressed block for all columns.
	if len(payload) < sizeCompressedBlockHead {
		return fmt.Errorf("payload too short %d (<%d)", len(payload), sizeCompressedBlockHead)
	}

	uncompressedSize := int(binary.BigEndian.Uint32(payload[0:sizeUncompressed]))
	compressedSize := int(binary.BigEndian.Uint32(payload[sizeUncompressed:sizeCompressedBlockHead]))
	payload = payload[sizeCompressedBlockHead:]

	if compressedSize > len(payload) {
		return fmt.Errorf("payload too short %d (<%d)", len(payload), compressedSize)
	}

	compressedData := payload[:compressedSize]
	// payload = payload[compressedSize:] // optional: if you want to validate trailing bytes

	// Allocate uncompressed buffer once (optionally reuse via a field/pool; see note below).
	uncompressedBuffer := make([]byte, uncompressedSize)

	if err := m.decompressBlock(compressedData, uncompressedBuffer); err != nil {
		return fmt.Errorf("decompress: %w", err)
	}

	// Optional sanity: ensure offsets are within buffer and the buffer is large enough.
	// Now ColBlockOffset is interpreted as an offset into this uncompressedBuffer.
	for col := 0; col < colNum; col++ {
		storageLen := int(m.StorageLen[col])
		colBlockOffset := int(m.ColBlockOffset[col])

		if colBlockOffset < 0 || colBlockOffset >= len(uncompressedBuffer) {
			return fmt.Errorf("col=%d: invalid ColBlockOffset=%d len(uncompressedBuffer)=%d",
				col, colBlockOffset, len(uncompressedBuffer))
		}

		// Column data must contain rowNum * storageLen bytes starting at colBlockOffset.
		need := rowNum * storageLen
		if colBlockOffset+need > len(uncompressedBuffer) {
			return fmt.Errorf("col=%d: insufficient column data: need %d bytes from offset %d (buf=%d)",
				col, need, colBlockOffset, len(uncompressedBuffer))
		}

		dataSlice := uncompressedBuffer[colBlockOffset : colBlockOffset+need]
		oid := m.ColOIDs[col]

		for row := 0; row < rowNum; row++ {
			rowData := dataSlice[:storageLen]
			dataSlice = dataSlice[storageLen:]

			txt, err := kwCellToText(oid, rowData)
			if err != nil {
				return fmt.Errorf("col=%d row=%d: %w", col, row, err)
			}
			m.ValuesTextRows[row][col] = txt
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
	case oidTimestampTZ, oidTimestamp, oidDate: // timestamptz/timestamp/date
		if len(cell) < 8 {
			return nil, fmt.Errorf("ts cell too short %d", len(cell))
		}
		//v := int64(binary.LittleEndian.Uint64(cell[:8]))
		//t := time.Unix(0, v*int64(time.Millisecond)).UTC()
		return cell[:8], nil

	case oidFloat8: // float8
		if len(cell) < 8 {
			return nil, fmt.Errorf("float8 cell too short %d", len(cell))
		}
		//f := math.Float64frombits(binary.LittleEndian.Uint64(cell[:8]))
		return cell[:8], nil

	case oidFloat4: // float4
		if len(cell) < 4 {
			return nil, fmt.Errorf("float4 cell too short %d", len(cell))
		}
		//f := math.Float32frombits(binary.LittleEndian.Uint32(cell[:4]))
		return cell[:4], nil

	case oidInt8: // int8
		if len(cell) < 8 {
			return nil, fmt.Errorf("int8 cell too short %d", len(cell))
		}
		//v := int64(binary.LittleEndian.Uint64(cell[:8]))
		return cell[:8], nil

	case oidInt4: // int4
		if len(cell) < 4 {
			return nil, fmt.Errorf("int4 cell too short %d", len(cell))
		}
		//v := int64(int32(binary.LittleEndian.Uint32(cell[:4])))
		return cell[:4], nil

	case oidText, oidVarchar, oidBpchar: // text/varchar/bpchar
		if len(cell) < 2 {
			return []byte{}, nil
		}
		b := cell[2:]
		for i, x := range b {
			if x == 0 {
				return b[:i], nil
			}
		}
		return b, nil

	default:
		dst := make([]byte, hex.EncodedLen(len(cell)))
		hex.Encode(dst, cell)
		return dst, nil
	}
}

// decompressBlock uncompress the data block
func (m *KwDataRowBatch) decompressBlock(compressed, uncompressed []byte) error {
	switch m.CompressionType {
	case CompressionTypeSnappy:
		_, err := snappy.Decode(uncompressed, compressed)
		if err != nil {
			return fmt.Errorf("decompress failed with snappy: %v", err)
		}
	case CompressionTypeLZ4:
		_, err := lz4.UncompressBlock(compressed, uncompressed)
		if err != nil {
			return fmt.Errorf("decompress failed with lz4: %v", err)
		}
	default:
		return fmt.Errorf("unknown compression type %d", m.CompressionType)
	}

	return nil
}
