package epmd

import (
	"net"

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
	socket     net.Listener
	port       uint16
	nodes      lib.Map[string, *registeredNode]
	terminated bool
	log        gen.Log
}

func tryStartServer(port uint16, log gen.Log) *server {

	srv := &server{
		port: port,
		log:  log,
	}

	return srv
}

func (s *server) resolve(name gen.Atom, docopy bool) ([]gen.Route, error) {
	var routes []gen.Route

	return routes, nil
}
