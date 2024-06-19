package dist

import (
	"ergo.services/ergo/gen"
	"sync"
)

func createLinks() *links {
	return &links{
		consumers: make(map[gen.PID][]gen.PID),
	}
}

type links struct {
	sync.RWMutex
	consumers map[gen.PID][]gen.PID
}

func (l *links) registerConsumer(target gen.PID, pid gen.PID) {
	l.Lock()
	defer l.Unlock()

	list := l.consumers[target]
	list = append(list, pid)
	l.consumers[target] = list
}

func (l *links) unregisterConsumer(target gen.PID, pid gen.PID) bool {
	l.Lock()
	defer l.Unlock()
	list := l.consumers[target]

	for i, p := range list {
		if pid != p {
			continue
		}
		list[0] = list[i]
		list = list[1:]
		if len(list) == 0 {
			delete(l.consumers, target)
			return true
		}
		l.consumers[target] = list
		return true
	}
	return false
}

func (l *links) unregister(target gen.PID) []gen.PID {
	l.Lock()
	defer l.Unlock()

	list := l.consumers[target]
	delete(l.consumers, target)

	return list
}
