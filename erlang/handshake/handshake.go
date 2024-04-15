package handshake

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"time"

	"ergo.services/ergo/gen"
)

type handshake struct {
	flags []Flag
}

type Options struct {
	Flags []Flag
}

func Create(options Options) gen.NetworkHandshake {
	handshake := &handshake{}
	handshake.flags = append(handshake.flags, options.Flags...)
	return handshake
}

func (h *handshake) Version() gen.Version {
	return Version
}

func (h *handshake) readMessage(conn net.Conn, timeout time.Duration, chunk []byte) ([]byte, error) {
	var b [4096]byte

	if timeout == 0 {
		conn.SetReadDeadline(time.Time{})
	}

	expect := 6
	for {
		if len(chunk) < expect {
			if timeout > 0 {
				conn.SetReadDeadline(time.Now().Add(timeout))
			}

			n, err := conn.Read(b[:])
			if err != nil {
				return nil, err
			}

			chunk = append(chunk, b[:n]...)
			continue
		}

		l := int(binary.BigEndian.Uint32(chunk[2:6]))
		if l > math.MaxUint16 {
			return nil, fmt.Errorf("too long handshake message")
		}

		if len(chunk) < 6+l {
			expect = 6 + l
			continue
		}

		return chunk, nil
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
