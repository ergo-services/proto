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

func (h *handshake) Start(node gen.NodeHandshake, conn net.Conn, options gen.HandshakeOptions) (gen.HandshakeResult, error) {
	var result gen.HandshakeResult
	var chunk []byte
	var message []byte
	var err error
	var challenge uint32

	result.ConnectionID = lib.RandomString(32)
	result.HandshakeVersion = h.Version()
	result.NodeFlags = options.Flags
	challenge = rand.Uint32()

	await := []byte{'s', 'n', 'N'}

	if h.version5 {
		if err := h.sendName(conn, node, options.Flags); err != nil {
			return result, err
		}
	} else {
		if err := h.sendNameVersion6(conn, node, options.Flags); err != nil {
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

			peerChallenge, err := h.readChallenge(message[1:], &result)
			if err != nil {
				return result, err
			}

			if err := h.sendChallengeReply(conn, peerChallenge, challenge, options.Cookie); err != nil {
				return result, err
			}

			await = []byte{'s', 'a'}

		case 'N':
			// The new challenge message format (version 6)
			// 8 (flags) + 4 (Creation) + 2 (NameLen) + Name
			if len(message) < 16 {
				return result, fmt.Errorf("malformed DIST handshake ('N' length)")
			}

			peerChallenge, err := h.readChallengeVersion6(message[1:], &result)
			if err != nil {
				return result, err
			}

			if h.version5 {
				// upgrade handshake to version 6 by sending complement message
				if err := h.sendComplement(conn, h.flags, node.Creation()); err != nil {
					return result, err
				}
			}

			if err := h.sendChallengeReply(conn, peerChallenge, challenge, options.Cookie); err != nil {
				return result, err
			}

			// add 's' (send_status message) for the case if we got it after 'n' or 'N' message
			await = []byte{'s', 'a'}

		case 'a':
			// 'a' + 16 (digest)
			if len(message) < 17 {
				return result, fmt.Errorf("malformed DIST handshake ('a' length of digest)")
			}

			// 'a' + 16 (digest)
			digest := genDigest(challenge, options.Cookie)
			if bytes.Compare(message[1:17], digest) != 0 {
				return result, fmt.Errorf("malformed DIST handshake ('a' digest)")
			}

			// handshaked
			return result, nil

		case 's':
			if string(message[1:3]) != "ok" {
				return result, fmt.Errorf("DIST handshake status != ok (%#v)", message[1:])
			}

			await = []byte{'n', 'N'}

		default:
			return result, fmt.Errorf("malformed DIST handshake (unknown message '%c')", message[0])
		}
	}
}

func (h *handshake) sendName(conn net.Conn, node gen.NodeHandshake, flags gen.NetworkFlags) error {
	f := h.flags
	if flags.Enable && flags.EnableRemoteSpawn {
		f = f.Enable(erlang23.FlagSpawn)
	}
	nodename := node.Name()
	_, tls := conn.(*tls.Conn)

	// byte + uint16 + uint32 + len(h.nodename)
	dataLength := uint32(1 + 2 + 4 + len(nodename))
	if tls {
		buf := make([]byte, dataLength+4)
		binary.BigEndian.PutUint32(buf[0:4], uint32(dataLength))
		buf[4] = 'n'
		binary.BigEndian.PutUint16(buf[5:7], 5)             // uint16
		binary.BigEndian.PutUint32(buf[7:11], f.ToUint32()) // uint32
		copy(buf[11:], []byte(nodename))
		if _, err := conn.Write(buf); err != nil {
			return err
		}
		return nil
	}

	buf := make([]byte, dataLength+2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(dataLength))
	buf[2] = 'n'
	binary.BigEndian.PutUint16(buf[3:5], 5)            // uint16
	binary.BigEndian.PutUint32(buf[5:9], f.ToUint32()) // uint32
	copy(buf[9:], []byte(nodename))
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	return nil
}

func (h *handshake) sendNameVersion6(conn net.Conn, node gen.NodeHandshake, flags gen.NetworkFlags) error {
	_, tls := conn.(*tls.Conn)
	creation := uint32(node.Creation())
	nodename := node.Name()
	f := h.flags
	if flags.Enable && flags.EnableRemoteSpawn {
		f = f.Enable(erlang23.FlagSpawn)
	}
	dataLength := 15 + len(nodename) // 1 + 8 (flags) + 4 (creation) + 2 (len nodename)

	if tls {
		buf := make([]byte, dataLength+4)
		binary.BigEndian.PutUint32(buf[0:4], uint32(dataLength))
		buf[4] = 'N'
		binary.BigEndian.PutUint64(buf[5:13], f.ToUint64())           // uint64
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
	binary.BigEndian.PutUint64(buf[3:11], f.ToUint64())           // uint64
	binary.BigEndian.PutUint32(buf[11:15], creation)              // uint32
	binary.BigEndian.PutUint16(buf[15:17], uint16(len(nodename))) // uint16
	copy(buf[17:], []byte(nodename))
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	return nil
}

func (h *handshake) readChallenge(b []byte, result *gen.HandshakeResult) (uint32, error) {
	var challenge uint32

	if len(b) < 15 {
		return challenge, fmt.Errorf("malformed DIST handshake challenge")
	}
	peerFlags := erlang23.Flags(binary.BigEndian.Uint32(b[2:6]))
	result.PeerFlags.Enable = true
	result.PeerFlags.EnableRemoteSpawn = peerFlags.IsEnabled(erlang23.FlagSpawn)
	result.Custom = erlang23.ConnectionOptions{
		NodeFlags: h.flags,
		PeerFlags: peerFlags,
	}

	version := binary.BigEndian.Uint16(b[0:2])
	if version != 5 {
		return challenge, fmt.Errorf("malformed DIST handshake version %d", version)
	}

	result.Peer = gen.Atom(b[10:])
	challenge = binary.BigEndian.Uint32(b[6:10])
	return challenge, nil
}

func (h *handshake) readChallengeVersion6(b []byte, result *gen.HandshakeResult) (uint32, error) {
	var challenge uint32
	peerFlags := erlang23.Flags(binary.BigEndian.Uint64(b[0:8]))
	result.PeerFlags.Enable = true
	result.PeerFlags.EnableRemoteSpawn = peerFlags.IsEnabled(erlang23.FlagSpawn)
	result.PeerCreation = int64(binary.BigEndian.Uint32(b[12:16]))
	result.Custom = erlang23.ConnectionOptions{
		NodeFlags: h.flags,
		PeerFlags: peerFlags,
	}

	challenge = binary.BigEndian.Uint32(b[8:12])

	lenName := int(binary.BigEndian.Uint16(b[16:18]))
	result.Peer = gen.Atom(b[18 : 18+lenName])

	return challenge, nil
}

func (h *handshake) sendChallengeReply(conn net.Conn, peerChallenge uint32, challenge uint32, cookie string) error {
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

	buf := make([]byte, dataLength+2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(dataLength))
	buf[2] = 'r'
	binary.BigEndian.PutUint32(buf[3:7], challenge) // uint32
	copy(buf[7:], digest)
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	return nil
}

func (h *handshake) sendComplement(conn net.Conn, f erlang23.Flags, creation int64) error {
	_, tls := conn.(*tls.Conn)
	node_flags := uint32(f.ToUint64() >> 32)
	dataLength := 9 // 1 + 4 (flag high) + 4 (creation)
	if tls {
		buf := make([]byte, dataLength+4)
		binary.BigEndian.PutUint32(buf[0:4], uint32(dataLength))
		buf[4] = 'c'
		binary.BigEndian.PutUint32(buf[5:9], node_flags)
		binary.BigEndian.PutUint32(buf[9:13], uint32(creation))
		if _, err := conn.Write(buf); err != nil {
			return err
		}
		return nil
	}

	buf := make([]byte, dataLength+2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(dataLength))
	buf[2] = 'c'
	binary.BigEndian.PutUint32(buf[3:7], node_flags)
	binary.BigEndian.PutUint32(buf[7:11], uint32(creation))
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	return nil
}
