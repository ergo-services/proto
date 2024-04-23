package dist

import (
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/lib"
)

// DIST proto
// https://www.erlang.org/doc/apps/erts/erl_dist_protocol#protocol-between-connected-nodes

type connection struct {
	id                  string
	creation            int64 // for uptime
	core                gen.Core
	log                 gen.Log
	node_flags          gen.NetworkFlags
	node_maxmessagesize int

	peer                gen.Atom
	peer_creation       int64
	peer_flags          gen.NetworkFlags
	peer_version        gen.Version
	peer_maxmessagesize int

	handshakeVersion gen.Version
	protoVersion     gen.Version

	recvQueues        []lib.QueueMPSC
	allocatedInQueues int64

	messagesIn  uint64
	messagesOut uint64
	bytesIn     uint64
	bytesOut    uint64

	dsn     string
	conn    net.Conn
	flusher io.Writer

	terminated bool

	wg sync.WaitGroup
}

//
// gen.RemoteNode implementation
//

func (c *connection) Name() gen.Atom {
	return c.peer
}

func (c *connection) Proxy() gen.Atom {
	return ""
}

func (c *connection) Uptime() int64 {
	return time.Now().Unix() - c.peer_creation
}
func (c *connection) Version() gen.Version {
	return c.peer_version
}

func (c *connection) Info() gen.RemoteNodeInfo {
	info := gen.RemoteNodeInfo{
		Node:             c.peer,
		Uptime:           0,
		ConnectionUptime: time.Now().Unix() - c.creation,
		Version:          c.peer_version,

		HandshakeVersion: c.handshakeVersion,
		ProtoVersion:     c.protoVersion,

		NetworkFlags: c.peer_flags,

		PoolSize: 1,
		PoolDSN:  []string{c.dsn},

		MessagesIn:  atomic.LoadUint64(&c.messagesIn),
		MessagesOut: atomic.LoadUint64(&c.messagesOut),

		BytesIn:  atomic.LoadUint64(&c.bytesIn),
		BytesOut: atomic.LoadUint64(&c.bytesOut),
	}
	return info
}

func (c *connection) Spawn(name gen.Atom, options gen.ProcessOptions, args ...any) (gen.PID, error) {
	opts := gen.ProcessOptionsExtra{
		ProcessOptions: options,
		ParentPID:      c.core.PID(),
		ParentLeader:   c.core.PID(),
		ParentLogLevel: c.core.LogLevel(),
		Args:           args,
	}
	if c.core.Security().ExposeEnvRemoteSpawn {
		opts.ParentEnv = c.core.EnvList()
	}
	return c.RemoteSpawn(name, opts)
}

func (c *connection) SpawnRegister(register gen.Atom, name gen.Atom, options gen.ProcessOptions, args ...any) (gen.PID, error) {
	opts := gen.ProcessOptionsExtra{
		ProcessOptions: options,
		ParentPID:      c.core.PID(),
		ParentLeader:   c.core.PID(),
		ParentLogLevel: c.core.LogLevel(),
		Register:       register,
		Args:           args,
	}
	if c.core.Security().ExposeEnvRemoteSpawn {
		opts.ParentEnv = c.core.EnvList()
	}
	return c.RemoteSpawn(name, opts)
}

func (c *connection) ApplicationStart(name gen.Atom, options gen.ApplicationOptions) error {
	return gen.ErrUnsupported
}
func (c *connection) ApplicationStartTemporary(name gen.Atom, options gen.ApplicationOptions) error {
	return gen.ErrUnsupported
}
func (c *connection) ApplicationStartTransient(name gen.Atom, options gen.ApplicationOptions) error {
	return gen.ErrUnsupported
}
func (c *connection) ApplicationStartPermanent(name gen.Atom, options gen.ApplicationOptions) error {
	return gen.ErrUnsupported
}

func (c *connection) Creation() int64 {
	return c.peer_creation
}

func (c *connection) Disconnect() {
	c.Terminate(gen.TerminateReasonNormal)
}

func (c *connection) ConnectionUptime() int64 {
	return time.Now().Unix() - c.creation
}

//
// gen.Connection implementation
//

func (c *connection) Node() gen.RemoteNode {
	return c
}

func (c *connection) SendPID(from gen.PID, to gen.PID, options gen.MessageOptions, message any) error {
	if to.Creation != c.peer_creation {
		return gen.ErrProcessIncarnation
	}

	return nil
}

func (c *connection) SendProcessID(from gen.PID, to gen.ProcessID, options gen.MessageOptions, message any) error {
	return nil
}

func (c *connection) SendAlias(from gen.PID, to gen.Alias, options gen.MessageOptions, message any) error {
	return nil
}

func (c *connection) SendEvent(from gen.PID, options gen.MessageOptions, message gen.MessageEvent) error {
	return gen.ErrUnsupported
}

func (c *connection) SendExit(from gen.PID, to gen.PID, reason error) error {
	return nil
}

func (c *connection) SendResponse(from gen.PID, to gen.PID, ref gen.Ref, options gen.MessageOptions, response any) error {
	return gen.ErrUnsupported
}

func (c *connection) SendTerminatePID(target gen.PID, reason error) error {
	return nil
}

func (c *connection) SendTerminateProcessID(target gen.ProcessID, reason error) error {
	return nil
}

func (c *connection) SendTerminateAlias(target gen.Alias, reason error) error {
	return nil
}

func (c *connection) SendTerminateEvent(target gen.Event, reason error) error {
	return nil
}

func (c *connection) CallPID(ref gen.Ref, from gen.PID, to gen.PID, options gen.MessageOptions, message any) error {
	return nil
}

func (c *connection) CallProcessID(ref gen.Ref, from gen.PID, to gen.ProcessID, options gen.MessageOptions, message any) error {
	return nil
}

func (c *connection) CallAlias(ref gen.Ref, from gen.PID, to gen.Alias, options gen.MessageOptions, message any) error {
	return nil
}

func (c *connection) LinkPID(pid gen.PID, target gen.PID) error {
	return nil
}

func (c *connection) UnlinkPID(pid gen.PID, target gen.PID) error {
	return nil
}

func (c *connection) LinkProcessID(pid gen.PID, target gen.ProcessID) error {
	return nil
}

func (c *connection) UnlinkProcessID(pid gen.PID, target gen.ProcessID) error {
	return nil
}

func (c *connection) LinkAlias(pid gen.PID, target gen.Alias) error {
	return nil
}

func (c *connection) UnlinkAlias(pid gen.PID, target gen.Alias) error {
	return nil
}

func (c *connection) LinkEvent(pid gen.PID, target gen.Event) ([]gen.MessageEvent, error) {
	return nil, nil
}

func (c *connection) UnlinkEvent(pid gen.PID, target gen.Event) error {
	return nil
}

func (c *connection) MonitorPID(pid gen.PID, target gen.PID) error {
	return nil
}

func (c *connection) DemonitorPID(pid gen.PID, target gen.PID) error {
	return nil
}

func (c *connection) MonitorProcessID(pid gen.PID, target gen.ProcessID) error {
	return nil
}

func (c *connection) DemonitorProcessID(pid gen.PID, target gen.ProcessID) error {
	return nil
}

func (c *connection) MonitorAlias(pid gen.PID, target gen.Alias) error {
	return nil
}

func (c *connection) DemonitorAlias(pid gen.PID, target gen.Alias) error {
	return nil
}

func (c *connection) MonitorEvent(pid gen.PID, target gen.Event) ([]gen.MessageEvent, error) {
	return nil, nil
}

func (c *connection) DemonitorEvent(pid gen.PID, target gen.Event) error {
	return nil
}

func (c *connection) RemoteSpawn(name gen.Atom, options gen.ProcessOptionsExtra) (gen.PID, error) {
	return gen.PID{}, nil
}

func (c *connection) Join(conn net.Conn, id string, dial gen.NetworkDial, tail []byte) error {
	if id != c.id {
		return fmt.Errorf("connection id mismatch")
	}

	if c.terminated {
		return fmt.Errorf("connection terminated")
	}

	c.conn = conn
	c.flusher = lib.NewFlusher(conn)

	c.wg.Add(1)
	go func() {

	re: // reconnected
		c.serve(tail)

		if c.terminated {
			c.wg.Done()
			return
		}

		if dial == nil {
			c.wg.Done()
			return
		}

		for i := 0; i < 3; i++ {
			c.log.Trace("re-dialing %s (attempt: %d)", c.dsn, i+1)
			nc, t, err := dial(c.dsn, id)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			c.conn = nc
			tail = t
			goto re
		}

		c.wg.Done()
		return
	}()

	return nil
}

func (c *connection) Terminate(reason error) {
	c.terminated = true
	c.conn.Close()
}

func (c *connection) serve(tail []byte) {

	recvN := 0
	recvNQ := len(c.recvQueues)

	buf := lib.TakeBuffer()
	buf.Append(tail)

	// remove the deadline
	c.conn.SetReadDeadline(time.Time{})

	for {
		// read packet
		buftail, err := c.read(c.conn, buf)
		if err != nil || buftail == nil {
			if err != nil {
				c.log.Trace("link with %s closed: %s", c.conn.RemoteAddr(), err)
			}
			lib.ReleaseBuffer(buf)
			c.conn.Close()
			return
		}

		recvN++

		atomic.AddUint64(&c.messagesIn, 1)
		atomic.AddUint64(&c.bytesIn, uint64(buf.Len()))
		// send 'buf' to the decoding queue
		qN := recvN % recvNQ
		if order := int(buf.B[6]); order > 0 {
			qN = order % recvNQ
		}
		c.log.Trace("received message. put it to pool[%d] of %s...", qN, c.conn.RemoteAddr())
		queue := c.recvQueues[qN]
		atomic.AddInt64(&c.allocatedInQueues, int64(buf.Cap()))

		queue.Push(buf)
		if queue.Lock() {
			go c.handleRecvQueue(queue)
		}
		buf = buftail

	}
}

func (c *connection) handleRecvQueue(q lib.QueueMPSC) {
	if lib.Recover() {
		defer func() {
			if r := recover(); r != nil {
				pc, fn, line, _ := runtime.Caller(2)
				c.log.Panic("panic on handling received message: %#v at %s[%s:%d]",
					r, runtime.FuncForPC(pc).Name(), fn, line)
				c.Terminate(gen.TerminateReasonPanic)
			}
		}()
	}

	c.log.Trace("start handling the message queue")
	for {
		v, ok := q.Pop()
		if ok == false {
			// no more items in the queue, unlock it
			q.Unlock()

			// but check the queue before the exit this goroutine
			if i := q.Item(); i == nil {
				return
			}

			// there is something in the queue, try to lock it back
			if locked := q.Lock(); locked == false {
				// another goroutine is started
				return
			}
			// get back to work
			continue
		}

		buf := v.(*lib.Buffer)

		// to avoid getting the buffer pool too big, we check the total volume (capacity)
		// we took from there and don't put it back if the limit has been reached.
		// releaseBuffer := true
		// if atomic.AddInt64(&c.allocatedInQueues, int64(-buf.Cap())) > limitMemRecvQueues {
		// 	releaseBuffer = false
		// }

		switch buf.B[7] {
		case 0:
		default:
			c.log.Error("unknown message type %d, ignored", buf.B[6])
			lib.ReleaseBuffer(buf)
		}

		// TODO
		// check if connection has been terminated
		// if c.terminated {
		//     return
		// }

	}
}

func (c *connection) read(conn net.Conn, buf *lib.Buffer) (*lib.Buffer, error) {
	return nil, nil
}

func (c *connection) wait() {
	c.wg.Wait()
}
