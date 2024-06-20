package erlang23

import (
	"ergo.services/ergo/gen"
	"ergo.services/ergo/lib"
	"fmt"
)

func factory_kernel() gen.ProcessBehavior {
	return &kernel{}
}

type kernel struct {
	GenServer
}

func (k *kernel) HandleInfo(msg any) error {
	return nil
}

func (k *kernel) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	k.Log().Info("got request from %s: %v", from, request)
	for i := 0; i < 5; i++ {
		rem := fmt.Sprintf("%s:%s", lib.RandomString(10), lib.RandomString(10))
		// pid, err := k.RemoteSpawn(from.Node, "erltest:go", gen.ProcessOptions{})
		pid, err := k.RemoteSpawn(from.Node, gen.Atom(rem), gen.ProcessOptions{})
		if err != nil {
			k.Log().Error("unable to spawn: %s", err)
		} else {
			k.Log().Info("spawned %s on %s", pid, pid.Node)
		}
	}
	return "ooook", nil
	// return k.PID(), nil
}
