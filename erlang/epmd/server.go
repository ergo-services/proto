package epmd

import (
	"ergo.services/ergo/gen"
	"ergo.services/ergo/lib"
)

type registeredNode struct {
	port   uint16
	hidden bool
	hi     uint16
	lo     uint16
	extra  []byte
}
type server struct {
	port       uint16
	nodes      lib.Map[string, *registeredNode]
	terminated bool
}

func tryStartServer(port uint16, log gen.Log) *server {

	srv := &server{}

	return srv
}
