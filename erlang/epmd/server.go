package epmd

import (
	"ergo.services/ergo/gen"
)

type server struct {
	terminated bool
}

func tryStartServer(port uint16, log gen.Log) *server {

	srv := &server{}

	return srv
}
