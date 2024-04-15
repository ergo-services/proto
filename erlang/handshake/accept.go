package handshake

import (
	"crypto/tls"
	"fmt"
	"net"

	"ergo.services/ergo/gen"
)

func (h *handshake) Accept(node gen.NodeHandshake, conn net.Conn, options gen.HandshakeOptions) (gen.HandshakeResult, error) {
	var result gen.HandshakeResult

	// check if this connection is secured by TLS
	_, tls := conn.(*tls.Conn)

	fmt.Println("HANDSHAKE Accept with TLS", tls)
	return result, nil

}
