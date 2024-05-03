package dist

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/lib"
	"ergo.services/proto/erlang/etf"
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

	cache   etf.AtomCache
	mapping *etf.AtomMapping

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
		Uptime:           0, // erlang has no this info
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
		return fmt.Errorf("DIST connection id mismatch")
	}

	if c.terminated {
		return fmt.Errorf("DIST connection terminated")
	}

	c.conn = conn
	c.flusher = lib.NewFlusher(conn)

	c.wg.Add(1)
	go func() {
		c.serve(tail)
		c.wg.Done()
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

	for {
		// read packet
		buftail, err := c.read(c.conn, buf)
		if err != nil || buftail == nil {
			if err != nil {
				c.log.Trace("DIST link with %s closed: %s", c.conn.RemoteAddr(), err)
			}
			lib.ReleaseBuffer(buf)
			c.conn.Close()
			return
		}

		recvN++

		atomic.AddUint64(&c.bytesIn, uint64(buf.Len()))
		// send 'buf' to the decoding queue
		qN := recvN % recvNQ
		if order := int(buf.B[6]); order > 0 {
			qN = order % recvNQ
		}
		c.log.Trace("received DIST message. put it to pool[%d] of %s...", qN, c.conn.RemoteAddr())
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
	var control, payload etf.Term

	if lib.Recover() {
		defer func() {
			if r := recover(); r != nil {
				pc, fn, line, _ := runtime.Caller(2)
				c.log.Panic("panic on handling received DIST message: %#v at %s[%s:%d]",
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
		control = nil
		payload = nil

		// check if connection has been terminated
		if c.terminated {
			return
		}

		if buf.Len() < 5 {
			c.log.Error("malformed DIST packet (too small), ignored")
			continue
		}

		if buf.B[4] != protoDist {
			c.log.Error("malformed DIST packet (unknown proto), ignored")
			continue
		}

		if buf.B[5] != protoDistMessage {
			c.log.Error("malformed DIST packet (unknown message type), ignored")
			continue
		}

		cache, packet, err := decodeDistHeaderAtomCache(buf.B[6:], c.cache.In)
		if err != nil {
			// TODO handle errMissingCache
			c.log.Error("malformed DIST packet (%s), ignored", err)
			continue
		}

		decodeOptions := etf.DecodeOptions{
			AtomMapping:   c.mapping,
			FlagBigPidRef: true,
		}

		// decode control message
		control, packet, err = etf.Decode(packet, cache, decodeOptions)
		if err != nil {
			c.log.Error("unable to decode DIST:ETF control (%s), ignored", err)
			continue
		}

		if len(packet) == 0 {
			// TODO control only
		}

		// decode payload message
		payload, packet, err = etf.Decode(packet, cache, decodeOptions)
		if err != nil {
			c.log.Error("unable to decode DIST:ETF payload (%s), ignored", err)
			continue
		}

		if len(packet) != 0 {
			c.log.Error("received DIST packet with extra %d byte(s), ignored", len(packet))
			continue
		}

		atomic.AddUint64(&c.messagesIn, 1)
		c.handleDistMessage(control, payload)
		lib.ReleaseBuffer(buf)
	}
}

func (c *connection) read(conn net.Conn, buf *lib.Buffer) (*lib.Buffer, error) {
	total := buf.Len()
	expect := 4
	for {
		if buf.Len() < expect {
			deadline := true
			if err := conn.SetReadDeadline(time.Now().Add(4 * keepAlivePeriod)); err != nil {
				deadline = false
			}
			n, e := buf.ReadDataFrom(conn, math.MaxUint16)
			if n == 0 {
				if err, ok := e.(net.Error); deadline && ok && err.Timeout() {
					c.log.Error("%s is not responding, drop connection", c.peer)
				}
				// link was closed
				return nil, nil
			}

			if e != nil {
				if e == io.EOF {
					// something went wrong
					return nil, nil
				}
				return nil, e
			}

			total += n
			// check if we should get more data
			continue
		}

		l := int(binary.BigEndian.Uint32(buf.B[:4]))

		if c.node_maxmessagesize > 0 && l > c.node_maxmessagesize {
			return nil, fmt.Errorf("received too long DIST message (len: %d, limit: %d)", l, c.node_maxmessagesize)
		}

		if l == 0 {
			// it was keepalive message
			expect = 4
			if buf.Len() == 4 {
				buf.Reset()
				continue
			}
			buf.B = buf.B[4:]
			continue
		}

		if lib.Trace() {
			c.log.Trace("...recv DIST buf.Len: %d, packet %d (expect: %d)", buf.Len(), l, expect)
		}

		if buf.Len() < l {
			expect = l
			continue
		}

		tail := lib.TakeBuffer()
		tail.Append(buf.B[l:total])

		buf.B = buf.B[:l]

		return tail, nil
	}
}

func (c *connection) wait() {
	c.wg.Wait()
}

func (c *connection) handleDistMessage(control etf.Term, payload etf.Term) (err error) {
	defer func() {
		if lib.Recover() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%s", r)
			}
		}
	}()

	switch t := control.(type) {
	case etf.Tuple:
		switch act := t.Element(1).(type) {
		case int:
			switch act {
			case distProtoREG_SEND:
				// {6, FromPid, Unused, ToName}
				to := gen.ProcessID{
					Node: c.peer,
					Name: t.Element(4).(gen.Atom),
				}
				options := gen.MessageOptions{}
				c.core.RouteSendProcessID(t.Element(2).(gen.PID), to, options, payload)
				return nil

			case distProtoSEND:
				// {2, Unused, ToPid}
				// SEND has no sender pid
				from := gen.PID{
					Node:     c.peer,
					Creation: c.peer_creation,
					ID:       1,
				}
				options := gen.MessageOptions{}
				c.core.RouteSendPID(from, t.Element(3).(gen.PID), options, payload)
				return nil

			case distProtoLINK:
				// {1, FromPid, ToPid}
				pid := t.Element(2).(gen.PID)
				target := t.Element(3).(gen.PID)
				c.core.RouteLinkPID(pid, target)
				return nil

			case distProtoUNLINK:
				// {4, FromPid, ToPid}
				pid := t.Element(2).(gen.PID)
				target := t.Element(3).(gen.PID)
				c.core.RouteUnlinkPID(pid, target)
				return nil

			case distProtoUNLINK_ID:
				// TODO
				return nil

			case distProtoUNLINK_ID_ACK:
				// TODO
				return nil

			case distProtoNODE_LINK:
				return nil

			case distProtoEXIT:
				// TODO
				// {3, FromPid, ToPid, Reason}
				terminated := t.Element(2).(gen.PID)
				// to := t.Element(3).(gen.PID)
				reason := fmt.Errorf("%s", t.Element(4))
				c.core.RouteTerminatePID(terminated, reason)
				return nil

			case distProtoEXIT2:
				return nil

			case distProtoMONITOR:
				// TODO
				// {19, FromPid, ToProc, Ref}, where FromPid = monitoring process
				// and ToProc = monitored process pid or name (atom)

				// fromPid := t.Element(2).(gen.PID)
				// ref := t.Element(4).(gen.Ref)
				// // if monitoring by pid
				// if to, ok := t.Element(3).(gen.PID); ok {
				// 	c.core.RouteMonitorPID(fromPid, to, ref)
				// 	return nil
				// }
				//
				// // if monitoring by process name
				// if to, ok := t.Element(3).(gen.Atom); ok {
				// 	processID := gen.ProcessID{
				// 		Node: c.core.Name(),
				// 		Name: to,
				// 	}
				// 	c.core.RouteMonitorProcessID(fromPid, processID, ref)
				// 	return nil
				// }

				return fmt.Errorf("malformed monitor message")

			case distProtoDEMONITOR:
				// TODO
				// {20, FromPid, ToProc, Ref}, where FromPid = monitoring process
				// and ToProc = monitored process pid or name (atom)
				// ref := t.Element(4).(gen.Ref)
				// fromPid := t.Element(2).(gen.PID)
				// c.core.RouteDemonitor(fromPid, ref)
				return nil

			case distProtoMONITOR_EXIT:
				// {21, FromProc, ToPid, Ref, Reason}, where FromProc = monitored process
				// pid or name (atom), ToPid = monitoring process, and Reason = exit reason for the monitored process
				reason := fmt.Errorf("%s", t.Element(5))
				// ref := t.Element(4).(gen.Ref)
				switch terminated := t.Element(2).(type) {
				case gen.PID:
					c.core.RouteTerminatePID(terminated, reason)
					return nil
				case gen.Atom:
					target := gen.ProcessID{Name: terminated, Node: c.peer}
					c.core.RouteTerminateProcessID(target, reason)
					return nil
				}
				return fmt.Errorf("malformed monitor exit message")

			case distProtoSEND_SENDER:
				// not supported. ignored
				return nil
			case distProtoPAYLOAD_EXIT:
				// not supported. ignored
				return nil
			case distProtoPAYLOAD_EXIT2:
				// not supported. ignored
				return nil
			case distProtoPAYLOAD_MONITOR_P_EXIT:
				// not supported. ignored
				return nil

			case distProtoALIAS_SEND:
				// {33, FromPid, Alias}
				alias := gen.Alias(t.Element(3).(gen.Ref))
				options := gen.MessageOptions{}
				c.core.RouteSendAlias(t.Element(2).(gen.PID), alias, options, payload)
				return nil

			case distProtoSPAWN_REQUEST:
				// TODO
				// {29, ReqId, From, GroupLeader, {Module, Function, Arity}, OptList}
				// registerName := ""
				// for _, option := range t.Element(6).(etf.List) {
				// 	name, ok := option.(etf.Tuple)
				// 	if !ok || len(name) != 2 {
				// 		return fmt.Errorf("malformed spawn request")
				// 	}
				// 	switch name.Element(1) {
				// 	case gen.Atom("name"):
				// 		registerName = string(name.Element(2).(gen.Atom))
				// 	}
				// }
				//
				// from := t.Element(3).(gen.PID)
				// ref := t.Element(2).(gen.Ref)
				//
				// mfa := t.Element(5).(etf.Tuple)
				// module := mfa.Element(1).(gen.Atom)
				// function := mfa.Element(2).(gen.Atom)
				// var args etf.List
				// if str, ok := payload.(string); !ok {
				// 	args, _ = payload.(etf.List)
				// } else {
				// 	// stupid Erlang's strings :). [1,2,3,4,5] sends as a string.
				// 	// args can't be anything but etf.List.
				// 	for i := range []byte(str) {
				// 		args = append(args, str[i])
				// 	}
				// }
				//
				// spawnRequestOptions := gen.RemoteSpawnOptions{
				// 	Name:     registerName,
				// 	Function: string(function),
				// }
				// spawnRequest := gen.RemoteSpawnRequest{
				// 	From:    from,
				// 	Ref:     ref,
				// 	Options: spawnRequestOptions,
				// }
				// dc.router.RouteSpawnRequest(dc.nodename, string(module), spawnRequest, args...)
				return nil

			case distProtoSPAWN_REPLY:
				// TODO
				// {31, ReqId, To, Flags, Result}
				// ref := t.Element(2).(gen.Ref)
				// to := t.Element(3).(gen.PID)
				// c.core.RouteSpawnReply(to, ref, t.Element(5))
				return nil

			default:
				return fmt.Errorf("unknown control command %#v", control)
			}
		}
	}

	return fmt.Errorf("unsupported control message %#v", control)
}
