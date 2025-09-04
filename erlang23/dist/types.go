package dist

import (
	"runtime/debug"

	"ergo.services/ergo/gen"
)

const (
	protoName    string = "DIST" // Erlang DIST protocol
	protoRelease string = "R6"   //

	// Distributed operations codes (http://www.erlang.org/doc/apps/erts/erl_dist_protocol.html)
	distProtoLINK                   = 1
	distProtoSEND                   = 2
	distProtoEXIT                   = 3
	distProtoUNLINK                 = 4
	distProtoNODE_LINK              = 5
	distProtoREG_SEND               = 6
	distProtoGROUP_LEADER           = 7
	distProtoEXIT2                  = 8
	distProtoSEND_TT                = 12
	distProtoEXIT_TT                = 13
	distProtoREG_SEND_TT            = 16
	distProtoEXIT2_TT               = 18
	distProtoMONITOR                = 19
	distProtoDEMONITOR              = 20
	distProtoMONITOR_EXIT           = 21
	distProtoSEND_SENDER            = 22
	distProtoSEND_SENDER_TT         = 23
	distProtoPAYLOAD_EXIT           = 24
	distProtoPAYLOAD_EXIT_TT        = 25
	distProtoPAYLOAD_EXIT2          = 26
	distProtoPAYLOAD_EXIT2_TT       = 27
	distProtoPAYLOAD_MONITOR_P_EXIT = 28
	distProtoSPAWN_REQUEST          = 29
	distProtoSPAWN_REQUEST_TT       = 30
	distProtoSPAWN_REPLY            = 31
	distProtoSPAWN_REPLY_TT         = 32
	distProtoALIAS_SEND             = 33
	distProtoALIAS_SEND_TT          = 34
	distProtoUNLINK_ID              = 35
	distProtoUNLINK_ID_ACK          = 36

	// http://erlang.org/doc/apps/erts/erl_ext_dist.html#distribution_header
	protoDist          = 131
	protoDistMessage   = 68
	protoDistFragment1 = 69
	protoDistFragmentN = 70

	defaultFragmentationUnit = 65000
)

var (
	Version = gen.Version{
		Name:    protoName,
		Release: protoRelease,
		License: gen.LicenseMIT,
	}
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				Version.Commit = setting.Value
				break
			}
		}
	}
}
