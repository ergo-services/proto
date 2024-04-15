package handshake

import (
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"time"

	"ergo.services/ergo/gen"
)

type handshake struct {
	flags flags
}

type Options struct {
	Flags []Flag
}

func Create(options Options) gen.NetworkHandshake {
	handshake := &handshake{}
	if len(options.Flags) == 0 {
		options.Flags = DefaultFlags()
	}
	handshake.flags = toFlags(options.Flags...)
	return handshake
}

func (h *handshake) Version() gen.Version {
	return Version
}

func (h *handshake) readMessage(conn net.Conn, timeout time.Duration, chunk []byte) ([]byte, []byte, error) {
	var b [4096]byte

	if timeout == 0 {
		conn.SetReadDeadline(time.Time{})
	}

	// http://erlang.org/doc/apps/erts/erl_dist_protocol.html#distribution-handshake
	// Every message in the handshake starts with a 16-bit big-endian integer,
	// which contains the message length (not counting the two initial bytes).
	// In Erlang this corresponds to option {packet, 2} in gen_tcp(3). Notice
	// that after the handshake, the distribution switches to 4 byte packet headers.
	expect := 2
	// check if this connection is secured by TLS
	_, tls := conn.(*tls.Conn)
	if tls {
		// TLS connection has 4 bytes packet length header
		expect = 4
	}

	header := expect

	for {
		if expect > math.MaxUint16 {
			return nil, nil, fmt.Errorf("too long DIST handshake message")
		}

		if len(chunk) < expect {
			if timeout > 0 {
				conn.SetReadDeadline(time.Now().Add(timeout))
			}

			n, err := conn.Read(b[:])
			if err != nil {
				return nil, nil, err
			}

			chunk = append(chunk, b[:n]...)
			continue
		}

		mlen := 0
		if header == 2 {
			mlen = int(binary.BigEndian.Uint16(chunk[:header]))
		} else {
			mlen = int(binary.BigEndian.Uint32(chunk[:header]))
		}

		if len(chunk) < header+mlen {
			expect = header + mlen
			continue
		}

		message := chunk[header:expect]
		tail := chunk[expect:]
		return message, tail, nil
	}
}

func DefaultFlags() []Flag {
	return []Flag{
		FlagPublished,
		FlagUnicodeIO,
		FlagDistMonitor,
		FlagNewFloats,
		FlagBitBinaries,
		FlagDistMonitorName,
		FlagExtendedPidsPorts,
		FlagExtendedReferences,
		FlagAtomCache,
		FlagHiddenAtomCache,
		FlagFunTags,
		FlagNewFunTags,
		FlagExportPtrTag,
		FlagSmallAtomTags,
		FlagUTF8Atoms,
		FlagMapTag,
		FlagHandshake23,
		FlagDistHdrAtomCache,
		FlagFragments,
		FlagBigCreation,
		FlagAlias,
		FlagV4NC,
	}
}

func toFlags(f ...Flag) flags {
	var fs uint64
	for _, v := range f {
		fs |= uint64(v)
	}
	return flags(fs)
}

func genDigest(challenge uint32, cookie string) []byte {
	s := fmt.Sprintf("%s%d", cookie, challenge)
	digest := md5.Sum([]byte(s))
	return digest[:]
}
