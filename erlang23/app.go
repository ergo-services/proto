package erlang23

import (
	"ergo.services/ergo/gen"
)

func CreateApp() gen.ApplicationBehavior {
	return &supportApp{}
}

type supportApp struct{}

func (sa *supportApp) Load(node gen.Node, args ...any) (gen.ApplicationSpec, error) {
	return gen.ApplicationSpec{
		Name:        "erlang_support_app",
		Description: "Erlang Support Application",
		Group: []gen.ApplicationMemberSpec{
			{
				Factory: factory_kernel,
				Name:    "kernel",
			},
			{
				Factory: factory_global_name_server,
				Name:    "global_name_server",
			},
		},
		Mode: gen.ApplicationModePermanent,
	}, nil
}

func (sa *supportApp) Start(mode gen.ApplicationMode) {}
func (sa *supportApp) Terminate(reason error)         {}
