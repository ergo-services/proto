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
	"ergo.services/ergo/lib"
	"ergo.services/proto/erlang23"
)

func (h *handshake) Accept(node gen.NodeHandshake, conn net.Conn, options gen.HandshakeOptions) (gen.HandshakeResult, error) {
	var result gen.HandshakeResult
	var chunk []byte
	var message []byte
	var err error
	var challenge uint32

	result.ConnectionID = lib.RandomString(32)
	result.HandshakeVersion = h.Version()
	result.NodeFlags = options.Flags
	await := []byte{'n', 'N'}

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
			if len(message) < 8 {
				return result, fmt.Errorf("malformed DIST handshake ('n' length)")
			}

			version, err := h.readNameFlagsVersion(message[1:], &result)
			if err != nil {
				return result, err
			}
			if err := h.sendStatus(conn); err != nil {
				return result, err
			}

			if version == 6 {
				challenge, err = h.sendChallengeVersion6(conn, node)
				if err != nil {
					return result, err
				}

				await = []byte{'s', 'r', 'c'}
				continue
			}

			challenge, err = h.sendChallenge(conn, node)
			if err != nil {
				return result, err
			}
			await = []byte{'s', 'r'}

		case 'N':
			// The new challenge message format (version 6)
			// 8 (flags) + 4 (Creation) + 2 (NameLen) + Name
			if len(message) < 16 {
				return result, fmt.Errorf("malformed `dist handshake ('N' length)")
			}
			if err := h.readNameFlagsVersion6(message[1:], &result); err != nil {
				return result, err
			}
			if err := h.sendStatus(conn); err != nil {
				return result, err
			}
			challenge, err = h.sendChallengeVersion6(conn, node)
			if err != nil {
				return result, err
			}
			await = []byte{'s', 'r'}

		case 'c':
			if len(message) < 9 {
				return result, fmt.Errorf("malformed DIST handshake ('c' length)")
			}
			h.readComplement(message[1:], &result)
			await = []byte{'r'}

		case 's':
			if string(message[1:3]) != "ok" {
				return result, fmt.Errorf("DIST handshake status != ok")
			}

			await = []byte{'c', 'r'}

		case 'r':
			if len(message) < 19 {
				return result, fmt.Errorf("malformed DIST handshake ('r' length)")
			}

			peerChallenge, valid := h.validateChallengeReply(message[1:], challenge, options.Cookie)
			if valid == false {
				return result, fmt.Errorf("malformed DIST handshake ('r' invalid reply)")
			}

			if err := h.sendChallengeAck(conn, peerChallenge, options.Cookie); err != nil {
				return result, err
			}

			// handshaked
			return result, nil
		}
	}
}

func (h *handshake) sendStatus(conn net.Conn) error {
	buf := []byte{
		0, 0, 0, 3,
		's', 'o', 'k',
	}
	_, tls := conn.(*tls.Conn)
	if tls == false {
		buf = buf[2:]
	}
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	return nil
}

func (h *handshake) readComplement(msg []byte, result *gen.HandshakeResult) {
	result.PeerCreation = int64(binary.BigEndian.Uint32(msg[4:8]))
}

func (h *handshake) validateChallengeReply(b []byte, challenge uint32, cookie string) (uint32, bool) {
	peerChallenge := binary.BigEndian.Uint32(b[:4])
	digestB := b[4:]

	digestA := genDigest(challenge, cookie)
	return peerChallenge, bytes.Equal(digestA[:], digestB)
}

func (h *handshake) sendChallengeAck(conn net.Conn, challenge uint32, cookie string) error {
	dataLength := 17 // 'a' + 16 (digest)
	digest := genDigest(challenge, cookie)
	_, tls := conn.(*tls.Conn)
	if tls {
		buf := make([]byte, dataLength+4)
		binary.BigEndian.PutUint32(buf[0:4], uint32(dataLength))
		buf[4] = 'a'
		copy(buf[5:], digest)
		if _, err := conn.Write(buf); err != nil {
			return err
		}
		return nil
	}

	buf := make([]byte, dataLength+2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(dataLength))
	buf[2] = 'a'
	copy(buf[3:], digest)
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	return nil
}

func (h *handshake) readNameFlagsVersion(b []byte, result *gen.HandshakeResult) (int, error) {
	peerFlags := erlang23.Flags(binary.BigEndian.Uint32(b[2:6]))
	if peerFlags.IsEnabled(erlang23.FlagBigCreation) == false {
		// we do not support Erlang version earlier than OTP-23
		return 0, fmt.Errorf("unsupported Erlang version")
	}
	result.NodeFlags.Enable = true
	result.NodeFlags.EnableRemoteSpawn = peerFlags.IsEnabled(erlang23.FlagSpawn)
	result.Custom = erlang23.ConnectionOptions{
		NodeFlags: h.flags,
		PeerFlags: peerFlags,
	}
	version := int(binary.BigEndian.Uint16(b[0:2]))
	if version != 5 {
		return 0, fmt.Errorf("malformed version for DIST handshake: %d", version)
	}

	if peerFlags.IsEnabled(erlang23.FlagHandshake23) {
		version = 6
	}

	// Erlang node limits the node name length to 256 characters (not bytes).
	// Ergo's atom is limited by 255 bytes
	if len(b[6:]) > 255 {
		return 0, fmt.Errorf("too long node name")
	}
	result.Peer = gen.Atom(b[6:])
	return version, nil
}

func (h *handshake) readNameFlagsVersion6(b []byte, result *gen.HandshakeResult) error {
	result.PeerCreation = int64(binary.BigEndian.Uint32(b[8:12]))
	peerFlags := erlang23.Flags(binary.BigEndian.Uint64(b[0:8]))
	if peerFlags.IsEnabled(erlang23.FlagBigCreation) == false {
		// we do not support Erlang version earlier than OTP-23
		return fmt.Errorf("unsupported Erlang version")
	}
	result.PeerFlags.Enable = true
	result.PeerFlags.EnableRemoteSpawn = peerFlags.IsEnabled(erlang23.FlagSpawn)
	result.Custom = erlang23.ConnectionOptions{
		NodeFlags: h.flags,
		PeerFlags: peerFlags,
	}

	// see my prev comment about name len
	nameLen := int(binary.BigEndian.Uint16(b[12:14]))
	if nameLen > 255 {
		return fmt.Errorf("malformed node name")
	}
	nodename := string(b[14 : 14+nameLen])
	result.Peer = gen.Atom(nodename)

	return nil
}

func (h *handshake) sendChallenge(conn net.Conn, node gen.NodeHandshake) (uint32, error) {
	challenge := rand.Uint32()
	nodename := node.Name()
	_, tls := conn.(*tls.Conn)

	dataLength := uint32(11 + len(nodename))
	if tls {
		buf := make([]byte, dataLength+4)
		binary.BigEndian.PutUint32(buf[0:4], dataLength)
		buf[4] = 'n'
		binary.BigEndian.PutUint16(buf[5:7], 5)                   // uint16
		binary.BigEndian.PutUint32(buf[7:11], h.flags.ToUint32()) // uint32
		binary.BigEndian.PutUint32(buf[11:15], challenge)         // uint32
		copy(buf[15:], []byte(nodename))
		if _, err := conn.Write(buf); err != nil {
			return 0, err
		}
		return challenge, nil
	}

	buf := make([]byte, dataLength+4)
	binary.BigEndian.PutUint16(buf[0:2], uint16(dataLength))
	buf[2] = 'n'
	binary.BigEndian.PutUint16(buf[3:5], 5)                  // uint16
	binary.BigEndian.PutUint32(buf[5:9], h.flags.ToUint32()) // uint32
	binary.BigEndian.PutUint32(buf[9:13], challenge)         // uint32
	copy(buf[13:], []byte(nodename))
	if _, err := conn.Write(buf); err != nil {
		return 0, err
	}
	return challenge, nil
}

func (h *handshake) sendChallengeVersion6(conn net.Conn, node gen.NodeHandshake) (uint32, error) {

	challenge := rand.Uint32()
	nodename := node.Name()
	_, tls := conn.(*tls.Conn)
	// 1 ('N') + 8 (flags) + 4 (chalange) + 4 (creation) + 2 (len(dh.nodename))
	dataLength := 19 + len(nodename)
	if tls {
		buf := make([]byte, dataLength+4)
		binary.BigEndian.PutUint32(buf[0:4], uint32(dataLength))
		buf[4] = 'N'
		binary.BigEndian.PutUint64(buf[5:13], h.flags.ToUint64())       // uint64
		binary.BigEndian.PutUint32(buf[13:17], challenge)               // uint32
		binary.BigEndian.PutUint32(buf[17:21], uint32(node.Creation())) // uint32
		binary.BigEndian.PutUint16(buf[21:23], uint16(len(nodename)))   // uint16
		copy(buf[23:], []byte(nodename))

		if _, err := conn.Write(buf); err != nil {
			return 0, err
		}
		return challenge, nil
	}

	buf := make([]byte, dataLength+2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(dataLength))
	buf[2] = 'N'
	binary.BigEndian.PutUint64(buf[3:11], h.flags.ToUint64())       // uint64
	binary.BigEndian.PutUint32(buf[11:15], challenge)               // uint32
	binary.BigEndian.PutUint32(buf[15:19], uint32(node.Creation())) // uint32
	binary.BigEndian.PutUint16(buf[19:21], uint16(len(nodename)))   // uint16
	copy(buf[21:], []byte(nodename))
	if _, err := conn.Write(buf); err != nil {
		return 0, err
	}
	return challenge, nil
}
