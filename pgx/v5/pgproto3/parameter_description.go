package pgproto3

import (
	"bytes"
	"encoding/binary"
	"encoding/json"

	"github.com/jackc/pgx/v5/internal/pgio"
)

type ParameterDescription struct {
	ParameterOIDs []uint32
}

// Backend identifies this message as sendable by the PostgreSQL backend.
func (*ParameterDescription) Backend() {}

// Decode decodes src into dst. src must contain the complete message with the exception of the initial 1 byte message
// type identifier and 4 byte message length.
func (dst *ParameterDescription) Decode(src []byte) error {
	buf := bytes.NewBuffer(src)

	if buf.Len() < 2 {
		return &invalidMessageFormatErr{messageType: "ParameterDescription"}
	}

	// Reported parameter count will be incorrect when number of args is greater than uint16
	buf.Next(2)
	// Instead infer parameter count by remaining size of message
	parameterCount := buf.Len() / 4

	*dst = ParameterDescription{ParameterOIDs: make([]uint32, parameterCount)}

	for i := 0; i < parameterCount; i++ {
		dst.ParameterOIDs[i] = binary.BigEndian.Uint32(buf.Next(4))
	}

	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *ParameterDescription) Encode(dst []byte) []byte {
	dst = append(dst, 't')
	sp := len(dst)
	dst = pgio.AppendInt32(dst, -1)

	dst = pgio.AppendUint16(dst, uint16(len(src.ParameterOIDs)))
	for _, oid := range src.ParameterOIDs {
		dst = pgio.AppendUint32(dst, oid)
	}

	pgio.SetInt32(dst[sp:], int32(len(dst[sp:])))

	return dst
}

// MarshalJSON implements encoding/json.Marshaler.
func (src ParameterDescription) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type          string
		ParameterOIDs []uint32
	}{
		Type:          "ParameterDescription",
		ParameterOIDs: src.ParameterOIDs,
	})
}

type ParameterDescriptionEx struct {
	ParameterOIDs []uint32 // ts table create sequence
	TagIndex      int16    // index of tag start position
	PtagIDs       []uint16 // ptag index and sequence
	StorageLen    []uint32
}

// Backend identifies this message as sendable by the PostgreSQL backend.
func (*ParameterDescriptionEx) Backend() {}

// Decode decodes src into dst. src must contain the complete message with the exception of the initial 1 byte message
// type identifier and 4 byte message length.
func (dst *ParameterDescriptionEx) Decode(src []byte) error {
	buf := bytes.NewBuffer(src)

	if buf.Len() < 2 {
		return &invalidMessageFormatErr{messageType: "ParameterDescriptionEx"}
	}

	// Instead infer parameter count by remaining size of message
	parameterCount := binary.BigEndian.Uint16(buf.Next(2))

	*dst = ParameterDescriptionEx{ParameterOIDs: make([]uint32, parameterCount)}

	for i := uint16(0); i < parameterCount; i++ {
		dst.ParameterOIDs[i] = binary.BigEndian.Uint32(buf.Next(4))
	}

	dst.TagIndex = int16(binary.BigEndian.Uint16(buf.Next(2)))
	ptagCount := binary.BigEndian.Uint16(buf.Next(2))
	for i := uint16(0); i < ptagCount; i++ {
		dst.PtagIDs = append(dst.PtagIDs, binary.BigEndian.Uint16(buf.Next(2)))
	}

	lenCount := binary.BigEndian.Uint16(buf.Next(2))
	for i := uint16(0); i < lenCount; i++ {
		dst.StorageLen = append(dst.StorageLen, binary.BigEndian.Uint32(buf.Next(4)))
	}

	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *ParameterDescriptionEx) Encode(dst []byte) []byte {
	dst = append(dst, 'X')
	sp := len(dst)
	dst = pgio.AppendInt32(dst, -1)

	dst = pgio.AppendUint16(dst, uint16(len(src.ParameterOIDs)))
	for _, oid := range src.ParameterOIDs {
		dst = pgio.AppendUint32(dst, oid)
	}

	dst = pgio.AppendInt16(dst, src.TagIndex)

	dst = pgio.AppendUint16(dst, uint16(len(src.PtagIDs)))
	for _, id := range src.PtagIDs {
		dst = pgio.AppendUint16(dst, id)
	}

	dst = pgio.AppendUint16(dst, uint16(len(src.StorageLen)))
	for _, len := range src.StorageLen {
		dst = pgio.AppendUint32(dst, len)
	}

	pgio.SetInt32(dst[sp:], int32(len(dst[sp:])))

	return dst
}

// MarshalJSON implements encoding/json.Marshaler.
func (src ParameterDescriptionEx) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type          string
		ParameterOIDs []uint32
		TagIndex      int16
		PtagIDs       []uint16
	}{
		Type:          "ParameterDescriptionEx",
		ParameterOIDs: src.ParameterOIDs,
		TagIndex:      src.TagIndex,
		PtagIDs:       src.PtagIDs,
	})
}
