package handshake

import (
	"net"

	"ergo.services/ergo/gen"
)

func (h *handshake) Start(node gen.NodeHandshake, conn net.Conn, options gen.HandshakeOptions) (gen.HandshakeResult, error) {
	var result gen.HandshakeResult

	return result, nil
}
