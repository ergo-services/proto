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
	"ergo.services/proto/erlang23"
)

type handshake struct {
	flags    erlang23.Flags
	version5 bool
}

type Options struct {
	Flags []erlang23.Flag
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
		EnableRemoteSpawn: h.flags.IsEnabled(erlang23.FlagSpawn),
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

func DefaultFlags() []erlang23.Flag {
	return []erlang23.Flag{
		erlang23.FlagPublished,
		erlang23.FlagUnicodeIO,
		erlang23.FlagDistMonitor,
		erlang23.FlagNewFloats,
		erlang23.FlagBitBinaries,
		erlang23.FlagDistMonitorName,
		erlang23.FlagExtendedPidsPorts,
		erlang23.FlagExtendedReferences,

		// Obsolete (see https://www.erlang.org/doc/apps/erts/erl_dist_protocol#distribution-flags)
		//
		// erlang23.FlagAtomCache,
		// erlang23.FlagHiddenAtomCache,

		erlang23.FlagFunTags,
		erlang23.FlagNewFunTags,
		erlang23.FlagExportPtrTag,
		erlang23.FlagSmallAtomTags,
		erlang23.FlagUTF8Atoms,
		erlang23.FlagMapTag,
		erlang23.FlagHandshake23,
		erlang23.FlagUnlinkID,
		erlang23.FlagDistHdrAtomCache,
		erlang23.FlagFragments,
		erlang23.FlagBigCreation,

		// Dont see any reason to enable this feature. maybe later.
		// if we decide to go with this flag there must be hanled the following
		// message types:
		//    distProtoPAYLOAD_EXIT
		//    distProtoPAYLOAD_EXIT2
		//    distProtoPAYLOAD_MONITOR_P_EXIT
		//
		// erlang23.FlagExitPayload,

		erlang23.FlagAlias,
		erlang23.FlagV4NC,
		erlang23.FlagSpawn,
	}
}

func toFlags(f ...erlang23.Flag) erlang23.Flags {
	var fs uint64
	for _, v := range f {
		fs |= uint64(v)
	}
	return erlang23.Flags(fs)
}

func genDigest(challenge uint32, cookie string) []byte {
	s := fmt.Sprintf("%s%d", cookie, challenge)
	digest := md5.Sum([]byte(s))
	return digest[:]
}
