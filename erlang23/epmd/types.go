package epmd

import (
	"ergo.services/ergo/gen"
	"runtime/debug"
	"time"
)

const (
	registrarName    string = "EPMD"
	registrarRelease string = "R1"

	defaultKeepAlive time.Duration = 3 * time.Second

	defaultEPMDPort uint16 = 4369

	epmdAliveReq      = 120
	epmdAliveResp     = 121
	epmdAliveRespX    = 118
	epmdPortPleaseReq = 122
	epmdPortResp      = 119
	epmdNamesReq      = 110
)

var (
	Version = gen.Version{
		Name:    registrarName,
		Release: registrarRelease,
		License: gen.LicenseBSL1,
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
