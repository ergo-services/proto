package erlang23

import (
	"ergo.services/ergo/gen"
)

func factory_kernel() gen.ProcessBehavior {
	return &kernel{}
}

type kernel struct {
	GenServer
}

func (k *kernel) HandleInfo(msg any) error {
	// k.Log().Info("got message: %v", msg)
	return nil
}

func (k *kernel) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	from.ID = 111111
	if err := k.MonitorPID(from); err != nil {
		if err == gen.ErrTargetExist {
			k.DemonitorPID(from)
			return gen.Atom("ok"), nil
		}
		k.Log().Error("unable to create monitor: %s", err)
	}
	return request, nil
}
