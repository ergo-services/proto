package handshake

import (
	"net"

	"ergo.services/ergo/gen"
)

func (h *handshake) Join(node gen.NodeHandshake, conn net.Conn, id string, options gen.HandshakeOptions) ([]byte, error) {
	return nil, gen.ErrUnsupported
}
