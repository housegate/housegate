package chproto

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

// decodeException reads an Exception body from the wire.
//
// Exception is not revision-sensitive in practice — the layout does not vary
// with server revision — but ch-go's EncodeAware/DecodeAware pair is used for
// API consistency. The revision argument is passed through but ignored by
// ch-go's implementation.
func (c *Codec) decodeException() (*Exception, error) {
	e := &proto.Exception{}
	if err := e.DecodeAware(c.r, c.Revision()); err != nil {
		return nil, fmt.Errorf("%w: exception: %v", ErrDecode, err)
	}
	return e, nil
}

// WriteException encodes an Exception onto the wire.
//
// Unlike Query.EncodeAware, Exception.EncodeAware does NOT write the leading
// type VarUInt, so we prepend ServerCodeException explicitly before the body.
// This matches what ClickHouse server does when sending an exception to a client.
func (c *Codec) WriteException(e *Exception) error {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeException))
	e.EncodeAware(&buf, c.Revision())
	_, err := c.w.Write(buf.Buf)
	return err
}
