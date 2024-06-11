package handshake

import (
	"ergo.services/ergo/gen"
	"runtime/debug"
)

const (
	handshakeName    string = "DIST Handshake"
	handshakeRelease string = "R5/R6"
)

var (
	Version = gen.Version{
		Name:    handshakeName,
		Release: handshakeRelease,
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
