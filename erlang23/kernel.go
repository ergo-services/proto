package erlang

import (
	"ergo.services/ergo/gen"
)

func factory_kernel() gen.ProcessBehavior {
	return &kernel{}
}

type kernel struct {
	GenServer
}
