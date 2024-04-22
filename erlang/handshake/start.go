package handshake

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"time"

	"ergo.services/ergo/gen"
)

func (h *handshake) Start(node gen.NodeHandshake, conn net.Conn, options gen.HandshakeOptions) (gen.HandshakeResult, error) {
	var result gen.HandshakeResult
	var chunk []byte
	var message []byte
	var err error
	var challenge uint32

	result.HandshakeVersion = h.Version()
	result.NodeFlags = options.Flags
	challenge = rand.Uint32()

	df := h.flags
	if options.Flags.Enable && options.Flags.EnableRemoteSpawn {
		df = df.enable(FlagSpawn)
	}
	await := []byte{'n', 'N'}

	if h.version5 {
		if err := sendName(conn, node, df); err != nil {
			return result, err
		}
		await = []byte{'s', 'n', 'N'}
	} else {
		if err := sendNameVersion6(conn, node, df); err != nil {
			return result, err
		}
	}

	for {
		message, chunk, err = h.readMessage(conn, 5*time.Second, chunk)
		if err != nil {
			return result, err
		}

		if bytes.Count(await, message[0:1]) == 0 {
			return result, fmt.Errorf("malformed DIST handshake (wrong response %d)", message[0])
		}

		switch message[0] {
		case 'n':
			// 'n' + 2 (version) + 4 (flags) + 4 (challenge) + name...
			if len(message) < 13 {
				return result, fmt.Errorf("malformed DIST handshake ('n' length)")
			}

			peerChallenge, err := readChallenge(message[1:], &result)
			if err != nil {
				return result, err
			}

			if err := sendChallengeReply(conn, challenge, peerChallenge, options.Cookie); err != nil {
				return result, err
			}

			// add 's' status for the case if we got it after 'n' or 'N' message
			// yes, sometime it happens
			await = []byte{'s', 'a'}

		case 'N':

		case 's':
		}
	}
}

func sendName(conn net.Conn, node gen.NodeHandshake, df flags) error {
	nodename := node.Name()
	_, tls := conn.(*tls.Conn)

	// byte + uint16 + uint32 + len(h.nodename)
	dataLength := uint32(1 + 2 + 4 + len(nodename))
	if tls {
		buf := make([]byte, dataLength+4)
		binary.BigEndian.PutUint32(buf[0:4], uint32(dataLength))
		buf[4] = 'n'
		binary.BigEndian.PutUint16(buf[5:7], 5)              // uint16
		binary.BigEndian.PutUint32(buf[7:11], df.toUint32()) // uint32
		copy(buf[11:], []byte(nodename))
		if _, err := conn.Write(buf); err != nil {
			return err
		}
		return nil
	}

	buf := make([]byte, dataLength+2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(dataLength))
	buf[2] = 'n'
	binary.BigEndian.PutUint16(buf[3:5], 5)             // uint16
	binary.BigEndian.PutUint32(buf[5:9], df.toUint32()) // uint32
	copy(buf[9:], []byte(nodename))
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	return nil
}

func sendNameVersion6(conn net.Conn, node gen.NodeHandshake, df flags) error {
	_, tls := conn.(*tls.Conn)
	creation := uint32(node.Creation())
	nodename := node.Name()
	dataLength := 13 + len(nodename) // 1 + 8 (flags) + 4 (creation) + 2 (len nodename)

	if tls {
		buf := make([]byte, dataLength+4)
		binary.BigEndian.PutUint32(buf[0:4], uint32(dataLength))
		buf[4] = 'N'
		binary.BigEndian.PutUint64(buf[5:13], df.toUint64())          // uint64
		binary.BigEndian.PutUint32(buf[13:17], creation)              //uint32
		binary.BigEndian.PutUint16(buf[17:19], uint16(len(nodename))) // uint16
		copy(buf[19:], []byte(nodename))
		if _, err := conn.Write(buf); err != nil {
			return err
		}
		return nil
	}

	buf := make([]byte, dataLength+2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(dataLength))
	buf[2] = 'N'
	binary.BigEndian.PutUint64(buf[3:11], df.toUint64())          // uint64
	binary.BigEndian.PutUint32(buf[11:15], creation)              // uint32
	binary.BigEndian.PutUint16(buf[15:17], uint16(len(nodename))) // uint16
	copy(buf[17:], []byte(nodename))
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	return nil
}

func readChallenge(b []byte, result *gen.HandshakeResult) (uint32, error) {
	var challenge uint32
	// var distHandshakeVersion int = 5

	if len(b) < 15 {
		return challenge, fmt.Errorf("malformed DIST handshake challenge")
	}
	flags := flags(binary.BigEndian.Uint32(b[2:6]))
	result.PeerFlags.Enable = true
	result.PeerFlags.EnableRemoteSpawn = flags.isSet(FlagSpawn)

	version := binary.BigEndian.Uint16(b[0:2])
	if version != 5 {
		return challenge, fmt.Errorf("malformed DIST handshake version %d", version)
	}

	result.Custom = flags
	result.Peer = gen.Atom(b[10:])
	challenge = binary.BigEndian.Uint32(b[6:10])
	return challenge, nil
}

func sendChallengeReply(conn net.Conn, peerChallenge uint32, challenge uint32, cookie string) error {
	_, tls := conn.(*tls.Conn)
	digest := genDigest(peerChallenge, cookie)
	dataLength := 5 + len(digest) // 1 (byte) + 4 (challenge) + 16 (digest)
	if tls {
		buf := make([]byte, dataLength+4)
		binary.BigEndian.PutUint32(buf[0:4], uint32(dataLength))
		buf[4] = 'r'
		binary.BigEndian.PutUint32(buf[5:9], challenge) // uint32
		copy(buf[9:], digest)
		if _, err := conn.Write(buf); err != nil {
			return err
		}
		return nil
	}

	buf := make([]byte, dataLength+4)
	binary.BigEndian.PutUint16(buf[0:2], uint16(dataLength))
	buf[2] = 'r'
	binary.BigEndian.PutUint32(buf[3:7], challenge) // uint32
	copy(buf[7:], digest)
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	return nil
}
