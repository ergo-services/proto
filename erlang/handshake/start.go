package handshake

import (
	"crypto/tls"
	"fmt"
	"net"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/lib"
)

func (h *handshake) Start(node gen.NodeHandshake, conn net.Conn, options gen.HandshakeOptions) (gen.HandshakeResult, error) {
	var result gen.HandshakeResult

	// check if this connection is secured by TLS
	_, tls := conn.(*tls.Conn)

	fmt.Println("HANDSHAKE Start with TLS", tls)
	return result, nil
}

func (h *handshake) composeName(name gen.Atom, b *lib.Buffer, tls bool) {
	// flags := composeFlags(h.flags)
	// version := uint16(h.options.Version)
	// if tls {
	// 	b.Allocate(11)
	// 	dataLength := 7 + len(h.nodename) // byte + uint16 + uint32 + len(h.nodename)
	// 	binary.BigEndian.PutUint32(b.B[0:4], uint32(dataLength))
	// 	b.B[4] = 'n'
	// 	binary.BigEndian.PutUint16(b.B[5:7], version)           // uint16
	// 	binary.BigEndian.PutUint32(b.B[7:11], flags.toUint32()) // uint32
	// 	b.Append([]byte(h.nodename))
	// 	return
	// }
	//
	// b.Allocate(9)
	// dataLength := 7 + len(h.nodename) // byte + uint16 + uint32 + len(h.nodename)
	// binary.BigEndian.PutUint16(b.B[0:2], uint16(dataLength))
	// b.B[2] = 'n'
	// binary.BigEndian.PutUint16(b.B[3:5], version)          // uint16
	// binary.BigEndian.PutUint32(b.B[5:9], flags.toUint32()) // uint32
	// b.Append([]byte(h.nodename))
}
