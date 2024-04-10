package handshake

import (
	"ergo.services/ergo/gen"
)

type handshake struct {
}

type Options struct{}

func Create(options Options) gen.NetworkHandshake {
	return &handshake{}
}

func (h *handshake) Version() gen.Version {
	return Version
}
