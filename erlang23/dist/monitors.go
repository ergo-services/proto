package dist

import (
	"ergo.services/ergo/gen"
	"sync"
)

func createMonitors() *monitors {
	return &monitors{
		consumers: make(map[any][]monitor),
		refs:      make(map[gen.Ref]any),
	}
}

type monitor struct {
	pid gen.PID
	ref gen.Ref
}

type monitors struct {
	sync.RWMutex
	consumers map[any][]monitor // pid/name => []monitor
	refs      map[gen.Ref]any   // ref => pid/name
}

func (m *monitors) registerConsumer(target any, pid gen.PID, ref gen.Ref) {
	m.Lock()
	defer m.Unlock()

	list := m.consumers[target]
	monitor := monitor{
		pid: pid,
		ref: ref,
	}
	list = append(list, monitor)
	m.consumers[target] = list
	m.refs[ref] = target
}

func (m *monitors) unregisterConsumer(target any, pid gen.PID, ref gen.Ref) gen.Ref {
	m.Lock()
	defer m.Unlock()

	delete(m.refs, ref)
	list := m.consumers[target]

	for i, mon := range list {
		if pid != mon.pid {
			continue
		}
		list[0] = list[i]
		list = list[1:]
		if len(list) == 0 {
			delete(m.consumers, target)
			return mon.ref
		}

		delete(m.refs, mon.ref)
		m.consumers[target] = list
		return mon.ref
	}
	return ref
}

func (m *monitors) unregister(target any) []monitor {
	m.Lock()
	defer m.Unlock()

	list := m.consumers[target]
	for _, mon := range list {
		delete(m.refs, mon.ref)
	}
	delete(m.consumers, target)

	return list
}
