package dist

import (
	"fmt"

	"encoding/binary"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/lib"
	"ergo.services/proto/erlang23/etf"
)

var (
	errMalformedCache = fmt.Errorf("malformed AtomHeader cache")
	errMissingInCache = fmt.Errorf("missing in AtomHeader cache")
)

//  https://erlang.org/doc/apps/erts/erl_ext_dist.html#normal-distribution-header

func decodeDistHeaderAtomCache(packet []byte, cache *etf.AtomCacheIn) ([]gen.Atom, []byte, error) {
	var err error

	// number of atom references are present in package
	references := int(packet[0])
	if references == 0 {
		return nil, packet[1:], nil
	}

	cached := make([]gen.Atom, references)
	flagsLen := references/2 + 1
	if len(packet) < 1+flagsLen {
		// malformed
		return nil, nil, errMalformedCache
	}
	flags := packet[1 : flagsLen+1]

	// The least significant bit in a half byte is flag LongAtoms.
	// If it is set, 2 bytes are used for atom lengths instead of 1 byte
	// in the distribution header.
	headerAtomLength := 1 // if 'LongAtom' is not set

	// extract this bit. just increase headereAtomLength if this flag is set
	lastByte := flags[len(flags)-1]
	shift := uint((references & 0x01) * 4)
	headerAtomLength += int((lastByte >> shift) & 0x01)

	// 1 (number of references) + references/2+1 (length of flags)
	packet = packet[1+flagsLen:]

	for i := 0; i < references; i++ {
		if len(packet) < 1+headerAtomLength {
			// malformed
			return nil, nil, errMalformedCache
		}
		shift = uint((i & 0x01) * 4)
		flag := (flags[i/2] >> shift) & 0x0F
		isNewReference := flag&0x08 == 0x08
		idxReference := uint16(flag & 0x07)
		idxInternal := uint16(packet[0])
		idx := (idxReference << 8) | idxInternal

		if isNewReference {
			atomLen := uint16(packet[1])
			if headerAtomLength == 2 {
				atomLen = binary.BigEndian.Uint16(packet[1:3])
			}
			// extract atom
			packet = packet[1+headerAtomLength:]
			if len(packet) < int(atomLen) {
				// malformed
				return nil, nil, errMalformedCache
			}
			atom := gen.Atom(packet[:atomLen])
			// store in temporary cache for decoding
			cached[i] = atom

			// store in link' cache
			cache.Atoms[idx] = &atom
			packet = packet[atomLen:]
			continue
		}

		c := cache.Atoms[idx]
		if c == nil {
			packet = packet[1:]
			// decode the rest of this cache but set return err = errMissingInCache
			err = errMissingInCache
			continue
		}
		cached[i] = *c
		packet = packet[1:]
	}

	return cached, packet, err
}

func encodeDistHeaderAtomCache(b *lib.Buffer,
	senderAtomCache map[gen.Atom]etf.CacheItem,
	encodingAtomCache *etf.EncodingAtomCache) {

	n := encodingAtomCache.Len()
	b.AppendByte(byte(n)) // write NumberOfAtomCache
	if n == 0 {
		return
	}

	startPosition := len(b.B)
	lenFlags := n/2 + 1
	flags := b.Extend(lenFlags)
	flags[lenFlags-1] = 0 // clear last byte to make sure we have valid LongAtom flag

	for i := 0; i < len(encodingAtomCache.L); i++ {
		// clean internal name cache
		encodingAtomCache.Delete(encodingAtomCache.L[i].Name)

		shift := uint((i & 0x01) * 4)
		idxReference := byte(encodingAtomCache.L[i].ID >> 8) // SegmentIndex
		idxInternal := byte(encodingAtomCache.L[i].ID & 255) // InternalSegmentIndex

		cachedItem := senderAtomCache[encodingAtomCache.L[i].Name]
		if cachedItem.Encoded == false {
			idxReference |= 8 // set NewCacheEntryFlag
		}

		// the 'flags' slice could be changed if b.B was reallocated during the encoding atoms
		flags = b.B[startPosition : startPosition+lenFlags]
		// clean it up before reuse
		if shift == 0 {
			flags[i/2] = 0
		}
		flags[i/2] |= idxReference << shift

		if cachedItem.Encoded {
			b.AppendByte(idxInternal)
			continue
		}

		if encodingAtomCache.HasLongAtom {
			// 1 (InternalSegmentIndex) + 2 (length) + name
			allocLen := 1 + 2 + len(encodingAtomCache.L[i].Name)
			buf := b.Extend(allocLen)
			buf[0] = idxInternal
			binary.BigEndian.PutUint16(buf[1:3], uint16(len(encodingAtomCache.L[i].Name)))
			copy(buf[3:], encodingAtomCache.L[i].Name)
		} else {
			// 1 (InternalSegmentIndex) + 1 (length) + name
			allocLen := 1 + 1 + len(encodingAtomCache.L[i].Name)
			buf := b.Extend(allocLen)
			buf[0] = idxInternal
			buf[1] = byte(len(encodingAtomCache.L[i].Name))
			copy(buf[2:], encodingAtomCache.L[i].Name)
		}

		cachedItem.Encoded = true
		senderAtomCache[encodingAtomCache.L[i].Name] = cachedItem
	}

	if encodingAtomCache.HasLongAtom {
		shift := uint((n & 0x01) * 4)
		flags = b.B[startPosition : startPosition+lenFlags]
		flags[lenFlags-1] |= 1 << shift // set LongAtom = 1
	}
}
