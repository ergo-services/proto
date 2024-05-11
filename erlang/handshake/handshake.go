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
	"ergo.services/proto/erlang"
)

type handshake struct {
	flags    erlang.Flags
	version5 bool
}

type Options struct {
	Flags []erlang.Flag
	// UseVersion5 makes DIST handhshake to use 5th version (by default is 6)
	// Must be enabled for making connection with Erlang 22 and earlier
	UseVersion5 bool
}

func Create(options Options) gen.NetworkHandshake {
	handshake := &handshake{
		version5: options.UseVersion5,
	}
	if len(options.Flags) == 0 {
		options.Flags = DefaultFlags()
	}
	handshake.flags = toFlags(options.Flags...)
	return handshake
}

func (h *handshake) NetworkFlags() gen.NetworkFlags {
	return gen.NetworkFlags{
		Enable:            true,
		EnableRemoteSpawn: h.flags.IsEnabled(erlang.FlagSpawn),
	}
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

		expect = header + mlen
		if len(chunk) < expect {
			continue
		}

		message := chunk[header:expect]
		tail := chunk[expect:]
		return message, tail, nil
	}
}

func DefaultFlags() []erlang.Flag {
	return []erlang.Flag{
		erlang.FlagPublished,
		erlang.FlagUnicodeIO,
		erlang.FlagDistMonitor,
		erlang.FlagNewFloats,
		erlang.FlagBitBinaries,
		erlang.FlagDistMonitorName,
		erlang.FlagExtendedPidsPorts,
		erlang.FlagExtendedReferences,
		erlang.FlagAtomCache,
		erlang.FlagHiddenAtomCache,
		erlang.FlagFunTags,
		erlang.FlagNewFunTags,
		erlang.FlagExportPtrTag,
		erlang.FlagSmallAtomTags,
		erlang.FlagUTF8Atoms,
		erlang.FlagMapTag,
		erlang.FlagHandshake23,
		erlang.FlagUnlinkID,
		erlang.FlagDistHdrAtomCache,
		erlang.FlagFragments,
		erlang.FlagBigCreation,
		erlang.FlagAlias,
		erlang.FlagV4NC,
	}
}

func toFlags(f ...erlang.Flag) erlang.Flags {
	var fs uint64
	for _, v := range f {
		fs |= uint64(v)
	}
	return erlang.Flags(fs)
}

func genDigest(challenge uint32, cookie string) []byte {
	s := fmt.Sprintf("%s%d", cookie, challenge)
	digest := md5.Sum([]byte(s))
	return digest[:]
}
