package erlang23

const (
	// distribution flags are defined here https://erlang.org/doc/apps/erts/erl_dist_protocol.html#distribution-flags
	FlagPublished          Flag = 0x1
	FlagAtomCache          Flag = 0x2
	FlagExtendedReferences Flag = 0x4
	FlagDistMonitor        Flag = 0x8
	FlagFunTags            Flag = 0x10
	FlagDistMonitorName    Flag = 0x20
	FlagHiddenAtomCache    Flag = 0x40
	FlagNewFunTags         Flag = 0x80
	FlagExtendedPidsPorts  Flag = 0x100
	FlagExportPtrTag       Flag = 0x200
	FlagBitBinaries        Flag = 0x400
	FlagNewFloats          Flag = 0x800
	FlagUnicodeIO          Flag = 0x1000
	FlagDistHdrAtomCache   Flag = 0x2000
	FlagSmallAtomTags      Flag = 0x4000
	//	FlagCompressed                   = 0x8000 // erlang uses this flag for the internal purposes
	FlagUTF8Atoms   Flag = 0x10000
	FlagMapTag      Flag = 0x20000
	FlagBigCreation Flag = 0x40000

	// FlagBigSeqTraceLabels Flag = 0x100000 // we dont use it

	FlagExitPayload Flag = 0x400000 // since OTP.22 enable replacement for EXIT, EXIT2, MONITOR_P_EXIT
	FlagFragments   Flag = 0x800000
	FlagHandshake23 Flag = 0x1000000 // new connection setup handshake (version 6) introduced in OTP 23
	FlagUnlinkID    Flag = 0x2000000
	// for 64bit flags
	FlagSpawn  Flag = 1 << 32
	FlagNameMe Flag = 1 << 33
	FlagV4NC   Flag = 1 << 34
	FlagAlias  Flag = 1 << 35
)

type Flag uint64
type Flags Flag

func (fs Flags) ToUint32() uint32 {
	return uint32(fs)
}

func (fs Flags) ToUint64() uint64 {
	return uint64(fs)
}

func (fs Flags) IsEnabled(f Flag) bool {
	return (uint64(fs) & uint64(f)) != 0
}

func (fs Flags) Enable(f Flag) Flags {
	nfs := uint64(fs)
	nfs |= uint64(f)
	return Flags(nfs)
}

func (fs Flags) Disable(f Flag) Flags {
	nfs := uint64(fs)
	nfs &= ^(uint64(f))
	return Flags(nfs)
}

type ConnectionOptions struct {
	NodeFlags Flags
	PeerFlags Flags
}
