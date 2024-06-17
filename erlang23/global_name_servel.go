package erlang23

import (
	"ergo.services/ergo/gen"
)

func factory_global_name_server() gen.ProcessBehavior {
	return &global_name_server{}
}

type global_name_server struct {
	GenServer
}
