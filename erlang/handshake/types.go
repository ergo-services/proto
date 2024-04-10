package handshake

import (
	"ergo.services/ergo/gen"
)

type ConnectionOptions struct {
}

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
