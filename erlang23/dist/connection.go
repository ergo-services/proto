package dist

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/lib"
	"ergo.services/proto/erlang23"
	"ergo.services/proto/erlang23/etf"
)

type messageResult struct {
	Error  error
	Result any
	Ref    gen.Ref
}

// DIST proto
// https://www.erlang.org/doc/apps/erts/erl_dist_protocol#protocol-between-connected-nodes

type connection struct {
	id                  string
	creation            int64 // for uptime
	core                gen.Core
	log                 gen.Log
	node_flags          gen.NetworkFlags
	node_maxmessagesize int
	node_erlang_flags   erlang23.Flags

	peer                gen.Atom
	peer_creation       int64
	peer_flags          gen.NetworkFlags
	peer_version        gen.Version
	peer_maxmessagesize int
	peer_erlang_flags   erlang23.Flags

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

	cache         etf.AtomCache
	mapping       *etf.AtomMapping
	fragments     lib.Map[uint64, *fragmentedPacket]
	fragment_unit int
	fragment_id   int64

	monitors *monitors
	links    *links

	// check and clean lost fragments
	checkCleanPending  atomic.Bool
	checkCleanTimer    *time.Timer
	checkCleanTimeout  time.Duration // default is 5 seconds
	checkCleanDeadline time.Duration // how long we wait for the next fragment of the certain sequenceID. Default is 30 seconds

	// monitors
	monitorsPeerNode    lib.Map[gen.Ref, any] // gen.Ref => gen.PID or gen.Atom
	monitorsPeerNodeRef lib.Map[any, gen.Ref] // gen.PID, gen.Atom => gen.Ref
	monitorsNodePeer    lib.Map[any, gen.Ref]

	requestsMutex sync.RWMutex
	requests      map[gen.Ref]chan messageResult

	terminated bool

	wg sync.WaitGroup
}

type fragmentedPacket struct {
	sync.Mutex

	buffer           *lib.Buffer
	disordered       *lib.Buffer
	disorderedSlices map[uint64][]byte
	fragmentID       uint64
	lastUpdate       time.Time
}

var (
	keepAlivePeriod = 15 * time.Second
	keepAlivePacket = []byte{0, 0, 0, 0}
)

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
		Uptime:           c.Uptime(),
		ConnectionUptime: c.ConnectionUptime(),
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

	if options.ImportantDelivery {
		return gen.ErrUnsupported
	}

	control := etf.Tuple{distProtoSEND, gen.Atom(""), to}
	return c.send(control, message)
}

func (c *connection) SendProcessID(from gen.PID, to gen.ProcessID, options gen.MessageOptions, message any) error {
	if options.ImportantDelivery {
		return gen.ErrUnsupported
	}

	control := etf.Tuple{distProtoREG_SEND, from, gen.Atom(""), to.Name}
	return c.send(control, message)
}

func (c *connection) SendAlias(from gen.PID, to gen.Alias, options gen.MessageOptions, message any) error {
	if options.ImportantDelivery {
		return gen.ErrUnsupported
	}

	if c.peer_erlang_flags.IsEnabled(erlang23.FlagAlias) == false {
		return gen.ErrUnsupported
	}
	control := etf.Tuple{distProtoALIAS_SEND, from, to}
	return c.send(control, message)
}

func (c *connection) SendEvent(from gen.PID, options gen.MessageOptions, message gen.MessageEvent) error {
	return gen.ErrUnsupported
}

func (c *connection) SendExit(from gen.PID, to gen.PID, reason error) error {
	control := etf.Tuple{distProtoEXIT, from, to, reason}
	return c.send(control, nil)
}

func (c *connection) SendResponse(from gen.PID, to gen.PID, options gen.MessageOptions, response any) error {
	// checking for an alias flag (see gen_server.go)
	useAlias := options.Ref.ID[2]&(1<<3) == 1<<3
	if useAlias {
		// send reply using ref as an alias
		tag := etf.ListImproper{gen.Atom("alias"), options.Ref}
		message := etf.Tuple{tag, response}
		alias := gen.Alias(options.Ref)
		return c.SendAlias(from, alias, gen.MessageOptions{}, message)
	}
	// send reply as a regular message
	message := etf.Tuple{options.Ref, response}
	return c.SendPID(from, to, gen.MessageOptions{}, message)
}

func (c *connection) SendResponseError(from gen.PID, to gen.PID, options gen.MessageOptions, err error) error {
	return gen.ErrUnsupported
}

func (c *connection) SendTerminatePID(target gen.PID, reason error) error {
	// atom value must be < 255 chars (not bytes). dont care. let it be 255 bytes.
	r := gen.Atom(fmt.Sprintf("%.255s", reason))
	for _, mon := range c.monitors.unregister(target) {
		control := etf.Tuple{distProtoMONITOR_EXIT, target, mon.pid, mon.ref, r}
		c.send(control, nil)
	}
	// handle links
	for _, pid := range c.links.unregister(target) {
		control := etf.Tuple{distProtoEXIT, target, pid, r}
		c.send(control, nil)
	}
	return nil
}

func (c *connection) SendTerminateProcessID(target gen.ProcessID, reason error) error {
	// handle monitors
	r := gen.Atom(fmt.Sprintf("%.255s", reason))
	for _, mon := range c.monitors.unregister(target.Name) {
		control := etf.Tuple{distProtoMONITOR_EXIT, target.Name, mon.pid, mon.ref, r}
		c.send(control, nil)
	}
	return nil
}

func (c *connection) SendTerminateAlias(target gen.Alias, reason error) error {
	return gen.ErrUnsupported
}

func (c *connection) SendTerminateEvent(target gen.Event, reason error) error {
	return gen.ErrUnsupported
}

func (c *connection) CallPID(from gen.PID, to gen.PID, options gen.MessageOptions, request any) error {
	if to.Creation != c.peer_creation {
		return gen.ErrProcessIncarnation
	}
	control := etf.Tuple{distProtoSEND, gen.Atom(""), to}
	message := etf.Tuple{
		gen.Atom("$gen_call"),
		etf.Tuple{from, options.Ref},
		request,
	}
	return c.send(control, message)
}

func (c *connection) CallProcessID(from gen.PID, to gen.ProcessID, options gen.MessageOptions, request any) error {
	control := etf.Tuple{distProtoREG_SEND, from, gen.Atom(""), to.Name}
	message := etf.Tuple{
		gen.Atom("$gen_call"),
		etf.Tuple{from, options.Ref},
		request,
	}
	return c.send(control, message)
}

func (c *connection) CallAlias(from gen.PID, to gen.Alias, options gen.MessageOptions, request any) error {
	if options.ImportantDelivery {
		return gen.ErrUnsupported
	}

	if c.peer_erlang_flags.IsEnabled(erlang23.FlagAlias) == false {
		return gen.ErrUnsupported
	}
	control := etf.Tuple{distProtoALIAS_SEND, from, to}
	message := etf.Tuple{
		gen.Atom("$gen_call"),
		etf.Tuple{from, options.Ref},
		request,
	}
	return c.send(control, message)
}

func (c *connection) LinkPID(pid gen.PID, target gen.PID) error {
	if target.Creation != c.peer_creation {
		return gen.ErrProcessIncarnation
	}
	c.links.registerConsumer(target, pid)
	control := etf.Tuple{distProtoLINK, pid, target}
	return c.send(control, nil)
}

func (c *connection) UnlinkPID(pid gen.PID, target gen.PID) error {
	c.links.unregisterConsumer(target, pid)
	control := etf.Tuple{distProtoUNLINK, pid, target}
	return c.send(control, nil)
}

func (c *connection) LinkProcessID(pid gen.PID, target gen.ProcessID) error {
	return gen.ErrUnsupported
}

func (c *connection) UnlinkProcessID(pid gen.PID, target gen.ProcessID) error {
	return gen.ErrUnsupported
}

func (c *connection) LinkAlias(pid gen.PID, target gen.Alias) error {
	return gen.ErrUnsupported
}

func (c *connection) UnlinkAlias(pid gen.PID, target gen.Alias) error {
	return gen.ErrUnsupported
}

func (c *connection) LinkEvent(pid gen.PID, target gen.Event) ([]gen.MessageEvent, error) {
	return nil, gen.ErrUnsupported
}

func (c *connection) UnlinkEvent(pid gen.PID, target gen.Event) error {
	return gen.ErrUnsupported
}

func (c *connection) MonitorPID(pid gen.PID, target gen.PID) error {
	if target.Creation != c.peer_creation {
		return gen.ErrProcessIncarnation
	}
	ref := c.core.MakeRef()
	c.monitors.registerConsumer(target, pid, ref)
	control := etf.Tuple{distProtoMONITOR, pid, target, ref}
	return c.send(control, nil)
}

func (c *connection) DemonitorPID(pid gen.PID, target gen.PID) error {
	if target.Creation != c.peer_creation {
		return gen.ErrProcessIncarnation
	}
	ref := c.monitors.unregisterConsumer(target, pid, gen.Ref{})
	control := etf.Tuple{distProtoDEMONITOR, pid, target, ref}
	return c.send(control, nil)
}

func (c *connection) MonitorProcessID(pid gen.PID, target gen.ProcessID) error {
	ref := c.core.MakeRef()
	c.monitors.registerConsumer(target, pid, ref)
	control := etf.Tuple{distProtoMONITOR, pid, target.Name, ref}
	return c.send(control, nil)
}

func (c *connection) DemonitorProcessID(pid gen.PID, target gen.ProcessID) error {
	ref := c.monitors.unregisterConsumer(target, pid, gen.Ref{})
	control := etf.Tuple{distProtoDEMONITOR, pid, target.Name, ref}
	return c.send(control, nil)
}

func (c *connection) MonitorAlias(pid gen.PID, target gen.Alias) error {
	// Erlang doesn't support this feature
	return gen.ErrUnsupported
}

func (c *connection) DemonitorAlias(pid gen.PID, target gen.Alias) error {
	// Erlang doesn't support this feature
	return gen.ErrUnsupported
}

func (c *connection) MonitorEvent(pid gen.PID, target gen.Event) ([]gen.MessageEvent, error) {
	// Erlang doesn't support this feature
	return nil, gen.ErrUnsupported
}

func (c *connection) DemonitorEvent(pid gen.PID, target gen.Event) error {
	// Erlang doesn't support this feature
	return gen.ErrUnsupported
}

func (c *connection) RemoteSpawn(name gen.Atom, options gen.ProcessOptionsExtra) (gen.PID, error) {
	var nopid gen.PID
	if c.peer_erlang_flags.IsEnabled(erlang23.FlagSpawn) == false {
		return nopid, gen.ErrUnsupported
	}
	mf := strings.Split(string(name), ":")
	if len(mf) != 2 || len(mf[0]) == 0 || len(mf[1]) == 0 {
		return nopid, fmt.Errorf("incorrect name. expected format \"module:function\"")
	}

	spawnOptions := etf.List{}
	if options.Register != "" {
		spawnOptions = append(spawnOptions, etf.Tuple{gen.Atom("name"), options.Register})
	}

	ref := c.core.MakeRef()
	ch := make(chan messageResult)
	c.requestsMutex.Lock()
	c.requests[ref] = ch
	c.requestsMutex.Unlock()

	// {29, ReqId, From, GroupLeader, {Module, Function, Arity}, OptList}
	control := etf.Tuple{
		distProtoSPAWN_REQUEST,
		ref,
		options.ParentPID,
		options.ParentLeader,
		etf.Tuple{ // {module, function, arity}
			gen.Atom(mf[0]),
			gen.Atom(mf[1]),
			len(options.Args),
		},
		spawnOptions,
	}
	if len(options.Args) == 0 {
		options.Args = nil
	}
	if err := c.send(control, options.Args); err != nil {
		c.requestsMutex.Lock()
		delete(c.requests, ref)
		c.requestsMutex.Unlock()
		return nopid, err
	}

	// Erlang's remote spawn always returns a pid
	// https://github.com/erlang/otp/issues/8603#issuecomment-2185870109
	// Weird design, but it is what it is.

	result := c.waitResult(ref, ch)
	if result.Error != nil {
		return nopid, result.Error
	}
	pid, ok := result.Result.(gen.PID)
	if ok == false {
		return nopid, gen.ErrMalformed
	}
	return pid, nil
}

func (c *connection) Join(conn net.Conn, id string, dial gen.NetworkDial, tail []byte) error {
	if id != c.id {
		return fmt.Errorf("DIST connection id mismatch")
	}

	if c.terminated {
		return fmt.Errorf("DIST connection terminated")
	}

	c.conn = conn
	c.flusher = lib.NewFlusherWithKeepAlive(conn, keepAlivePacket, keepAlivePeriod)

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

		if buf.Len() < 5 {
			c.log.Error("malformed DIST packet (too small), ignored")
			c.conn.Close()
			return
		}

		if buf.B[4] != protoDist {
			c.log.Error("malformed DIST packet (unknown proto: %#v), ignored", buf.B)
			c.conn.Close()
			return
		}

		switch buf.B[5] {
		case protoDistMessage:
			if buf.B[6] == 0 {
				// no cache in there
				break
			}

			if _, _, err := decodeDistHeaderAtomCache(buf.B[6:], c.cache.In); err != nil {
				c.log.Error("malformed DIST message: %s", err)
				c.conn.Close()
				return
			}
		case protoDistFragment1:
			if _, _, err := decodeDistHeaderAtomCache(buf.B[23:], c.cache.In); err != nil {
				c.log.Error("malformed DIST fragment message: %s", err)
				c.conn.Close()
				return
			}

		case protoDistFragmentN:
			break

		default:
			c.log.Error("malformed DIST packet (unknown message type), ignored")
			continue
		}

		recvN++

		atomic.AddUint64(&c.bytesIn, uint64(buf.Len()))
		// send 'buf' to the decoding queue
		qN := recvN % recvNQ
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
	var control, payload any

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

	c.log.Trace("start handling message queue")
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

		switch buf.B[5] {
		case protoDistMessage:
			break
		case protoDistFragment1, protoDistFragmentN:
			assembled, err := c.decodeFragment(buf.B[6:])
			if err != nil {
				c.log.Error("malformed DIST fragment: %s", err)
				continue
			}
			if assembled == nil {
				continue
			}
			buf = assembled

		default:
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

		if len(packet) > 0 {

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
		}

		atomic.AddUint64(&c.messagesIn, 1)
		c.handleDistMessage(control, payload)
		lib.ReleaseBuffer(buf)
	}
}

func (c *connection) read(conn net.Conn, buf *lib.Buffer) (*lib.Buffer, error) {
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

		c.log.Trace("received DIST packet: buf.Len %d, packet %d, expect %d", buf.Len(), l, expect)

		if buf.Len() < l+4 {
			expect = l + 4
			continue
		}

		tail := lib.TakeBuffer()
		tail.Append(buf.B[l+4:])

		buf.B = buf.B[:l+4]

		return tail, nil
	}
}

func (c *connection) wait() {
	c.wg.Wait()
}

func (c *connection) handleDistMessage(control any, payload any) (err error) {
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
		case int64:
			switch act {
			case distProtoREG_SEND:
				// {6, FromPid, Unused, ToName}
				to := gen.ProcessID{
					Node: c.core.Name(),
					Name: t.Element(4).(gen.Atom),
				}
				from := t.Element(2).(gen.PID)
				options := gen.MessageOptions{}
				c.core.RouteSendProcessID(from, to, options, payload)
				return nil

			case distProtoSEND:
				// {2, Unused, ToPid}
				// SEND has no sender pid
				from := gen.PID{
					Node:     c.peer,
					Creation: c.peer_creation,
					ID:       1,
				}
				to := t.Element(3).(gen.PID)
				options := gen.MessageOptions{}

				// check if this message is reply for the call request:
				// etf.Tuple{gen.Ref, reply}
				if p, ok := payload.(etf.Tuple); ok && len(p) == 2 {
					if ref, ok := p.Element(1).(gen.Ref); ok {
						reply := p.Element(2)
						options.Ref = ref
						c.core.RouteSendResponse(from, to, options, reply)
						return nil
					}
				}

				c.core.RouteSendPID(from, to, options, payload)
				return nil

			case distProtoALIAS_SEND:
				// {33, FromPid, Alias}
				from := t.Element(2).(gen.PID)
				alias := gen.Alias(t.Element(3).(gen.Ref))
				options := gen.MessageOptions{}
				c.core.RouteSendAlias(from, alias, options, payload)
				return nil

			case distProtoLINK:
				// {1, FromPid, ToPid}
				pid := t.Element(2).(gen.PID)
				target := t.Element(3).(gen.PID)
				if err := c.core.RouteLinkPID(pid, target); err == nil {
					c.links.registerConsumer(target, pid)
					return nil
				}
				control := etf.Tuple{distProtoEXIT, target, pid, gen.Atom("noproc")}
				c.send(control, nil)
				return nil

			case distProtoUNLINK:
				// {4, FromPid, ToPid}
				pid := t.Element(2).(gen.PID)
				target := t.Element(3).(gen.PID)
				c.links.unregisterConsumer(target, pid)
				c.core.RouteUnlinkPID(pid, target)
				return nil

			case distProtoUNLINK_ID:
				// {35, Id, FromPid, ToPid}
				id := t.Element(2)
				pid := t.Element(3).(gen.PID)
				target := t.Element(4).(gen.PID)
				c.links.unregisterConsumer(target, pid)
				control := etf.Tuple{distProtoUNLINK_ID_ACK, id, target, pid}
				c.send(control, nil)
				return nil

			case distProtoUNLINK_ID_ACK:
				// {36, Id, FromPid, ToPid}
				// we dont need it
				return nil

			case distProtoNODE_LINK:
				// docs says nothing about this type of message
				// and i have no idea why it does exist?
				return nil

			case distProtoEXIT, distProtoEXIT2:
				// no idea why they created them the same
				// EXIT {3, FromPid, ToPid, Reason}
				// EXIT2 {8, FromPid, ToPid, Reason}
				pid := t.Element(2).(gen.PID)
				target := t.Element(3).(gen.PID)
				reason := fmt.Errorf("%s", t.Element(4))
				if c.links.unregisterConsumer(target, pid) == false {
					// it wasn't a link. remote process sent exit signal with erlang:send/2
					c.core.RouteSendExit(pid, target, reason)
					return nil
				}
				c.core.RouteTerminatePID(target, reason)
				return nil

			case distProtoMONITOR:
				// {19, FromPid, Target, Ref}, where FromPid = monitoring process
				// and Target = monitored process pid or name (atom)

				fromPid := t.Element(2).(gen.PID)
				ref := t.Element(4).(gen.Ref)
				// if monitoring by pid
				switch to := t.Element(3).(type) {
				case gen.PID:
					if err := c.core.RouteMonitorPID(fromPid, to); err == nil {
						c.monitors.registerConsumer(to, fromPid, ref)
						return nil
					}
					// unknown target => send DOWN
					control := etf.Tuple{distProtoMONITOR_EXIT, to, fromPid, ref, gen.Atom("DOWN")}
					c.send(control, nil)
					return nil

				case gen.Atom:
					// otherwise, target must be a process name
					target := gen.ProcessID{
						Name: to,
						Node: c.core.Name(),
					}
					if err := c.core.RouteMonitorProcessID(fromPid, target); err == nil {
						c.monitors.registerConsumer(to, fromPid, ref)
						return nil
					}
					// unknown target => send DOWN
					control := etf.Tuple{distProtoMONITOR_EXIT, to, fromPid, ref, gen.Atom("DOWN")}
					c.send(control, nil)
					return nil
				}

				return fmt.Errorf("malformed monitor message")

			case distProtoDEMONITOR:
				// {20, FromPid, ToProc, Ref}, where FromPid = monitoring process
				// and ToProc = monitored process pid or name (atom)
				ref := t.Element(4).(gen.Ref)
				fromPid := t.Element(2).(gen.PID)

				switch to := t.Element(3).(type) {
				case gen.PID:
					c.core.RouteDemonitorPID(fromPid, to)
					c.monitors.unregisterConsumer(to, fromPid, ref)
					return nil

				case gen.Atom:
					target := gen.ProcessID{
						Name: to,
						Node: c.core.Name(),
					}
					c.core.RouteDemonitorProcessID(fromPid, target)
					c.monitors.unregisterConsumer(to, fromPid, ref)
					return nil
				}
				return fmt.Errorf("malformed demonitor message")

			case distProtoMONITOR_EXIT:
				// {21, FromProc, ToPid, Ref, Reason}, where FromProc = monitored process
				// pid or name (atom), ToPid = monitoring process, and Reason = exit reason for the monitored process
				reason := fmt.Errorf("%s", t.Element(5))
				pid := t.Element(3).(gen.PID)
				ref := t.Element(4).(gen.Ref)
				switch target := t.Element(2).(type) {
				case gen.PID:
					c.monitors.unregisterConsumer(target, pid, ref)
					return c.core.RouteTerminatePID(target, reason)

				case gen.Atom:
					tgt := gen.ProcessID{Name: target, Node: c.peer}
					c.monitors.unregisterConsumer(tgt, pid, ref)
					return c.core.RouteTerminateProcessID(tgt, reason)
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

			case distProtoSPAWN_REQUEST:
				// {29, ReqId, From, GroupLeader, {Module, Function, Arity}, OptList}
				registerName := gen.Atom("")
				// OptList is a proplist
				for _, v := range t.Element(6).(etf.List) {
					prop := v.(etf.Tuple)
					if len(prop) != 2 {
						return fmt.Errorf("malformed spawn request")
					}
					name := prop.Element(1).(gen.Atom)
					switch name {
					case gen.Atom("name"):
						registerName = prop.Element(2).(gen.Atom)
						continue
					}
				}
				//
				ref := t.Element(2).(gen.Ref)
				from := t.Element(3).(gen.PID)
				leader := t.Element(3).(gen.PID)

				mfa := t.Element(5).(etf.Tuple)
				module := mfa.Element(1).(gen.Atom)

				args := []any{}
				switch pl := payload.(type) {
				case string:
					// Erlang's strings: args like [1,2,3,4,5] Erlang sends as a string.
					// Yeah, I know :). It makes me crazy too.
					// Args can't be anything but etf.List.
					for _, b := range []byte(pl) {
						args = append(args, int8(b))
					}
				case etf.List:
					for _, a := range pl {
						args = append(args, a)
					}
				}

				spawnOptions := gen.ProcessOptionsExtra{
					ParentPID:    from,
					ParentLeader: leader,
					Register:     registerName,
					Args:         args,
				}

				spawnedPID, err := c.core.RouteSpawn(c.core.Name(), module, spawnOptions, c.peer)

				// {31, ReqId, To, Flags, Result}
				if err != nil {
					control := etf.Tuple{
						distProtoSPAWN_REPLY,
						ref,
						from,
						0,                     // Flags
						gen.Atom(err.Error()), // Result
					}
					c.send(control, nil)
					return nil
				}

				control := etf.Tuple{
					distProtoSPAWN_REPLY,
					ref,
					from,
					0,          // Flags
					spawnedPID, // Result
				}
				c.send(control, nil)
				return nil

			case distProtoSPAWN_REPLY:
				result := messageResult{}
				// {31, ReqId, To, Flags, Result}
				result.Ref = t.Element(2).(gen.Ref)
				c.requestsMutex.RLock()
				ch, found := c.requests[result.Ref]
				c.requestsMutex.RUnlock()
				if found == false {
					// late reply
					return nil
				}

				switch r := t.Element(5).(type) {
				case gen.PID:
					result.Result = r
				case gen.Atom:
					result.Error = fmt.Errorf("%s", string(r))
				}

				select {
				case ch <- result:
				default:
				}

				return nil

			default:
				return fmt.Errorf("unknown control command %#v", control)
			}
		}
	}

	return fmt.Errorf("unsupported control message %#v", control)
}

func (c *connection) decodeFragment(fragment []byte) (*lib.Buffer, error) {
	var first bool

	sequenceID := binary.BigEndian.Uint64(fragment[1:9])
	fragmentID := binary.BigEndian.Uint64(fragment[9:17])
	if fragmentID == 0 {
		return nil, fmt.Errorf("fragmentID can't be 0")
	}

	first = fragment[0] == protoDistFragment1
	fragment = fragment[17:]

	packet, exist := c.fragments.Load(sequenceID)
	if exist == false {
		packet = &fragmentedPacket{
			buffer:           lib.TakeBuffer(),
			disordered:       lib.TakeBuffer(),
			disorderedSlices: make(map[uint64][]byte),
			lastUpdate:       time.Now(),
		}

		packet.buffer.AppendByte(protoDistMessage)
		c.fragments.Store(sequenceID, packet)
	}
	packet.Lock()

	// until we get the first item everything will be treated as disordered
	if first {
		packet.fragmentID = fragmentID + 1
	}

	if packet.fragmentID-fragmentID != 1 {
		// got the next fragment. disordered
		slice := packet.disordered.Extend(len(fragment))
		copy(slice, fragment)
		packet.disorderedSlices[fragmentID] = slice
	} else {
		// order is correct. just append
		packet.buffer.Append(fragment)
		packet.fragmentID = fragmentID
	}

	// check whether we have disordered slices and try
	// to append them if it does fit

	if packet.fragmentID > 0 && len(packet.disorderedSlices) > 0 {
		for i := packet.fragmentID - 1; i > 0; i-- {
			if slice, ok := packet.disorderedSlices[i]; ok {
				packet.buffer.Append(slice)
				delete(packet.disorderedSlices, i)
				packet.fragmentID = i
				continue
			}
			break
		}
	}

	packet.lastUpdate = time.Now()
	packet.Unlock()

	if packet.fragmentID == 1 && len(packet.disorderedSlices) == 0 {
		// it was the last fragment
		c.fragments.Delete(sequenceID)
		lib.ReleaseBuffer(packet.disordered)
		return packet.buffer, nil
	}

	if c.checkCleanPending.CompareAndSwap(false, true) {
		return nil, nil
	}

	if c.checkCleanTimer != nil {
		c.checkCleanTimer.Reset(c.checkCleanTimeout)
		return nil, nil
	}

	c.checkCleanTimer = time.AfterFunc(c.checkCleanTimeout, func() {
		if c.fragments.Len() == 0 {
			c.checkCleanPending.Store(false)
			return
		}

		valid := time.Now().Add(-c.checkCleanDeadline)
		c.fragments.RangeLock(func(_ uint64, v *fragmentedPacket) bool {
			if v.lastUpdate.Before(valid) {
				// dropping  due to exceeded deadline
				c.fragments.DeleteNoLock(sequenceID)
			}

			return true
		})

		if c.fragments.Len() == 0 {
			c.checkCleanPending.Store(false)
			return
		}

		c.checkCleanPending.Store(true)
		c.checkCleanTimer.Reset(c.checkCleanTimeout)
	})

	return nil, nil
}

func (c *connection) send(control, payload any) error {
	packetBuffer := lib.TakeBuffer()
	lenMessage, lenAtomCache, lenPacket := 0, 0, 0

	reserve := 1024
	packetBuffer.Allocate(reserve)
	startDataPosition := reserve

	fragmentationEnabled := c.peer_erlang_flags.IsEnabled(erlang23.FlagFragments)

	encodingOptions := etf.EncodeOptions{
		AtomMapping:     c.mapping,
		NodeName:        string(c.core.Name()),
		PeerName:        string(c.peer),
		FlagBigPidRef:   c.peer_erlang_flags.IsEnabled(erlang23.FlagV4NC),
		FlagBigCreation: c.peer_erlang_flags.IsEnabled(erlang23.FlagBigCreation),
	}

	// encode Control
	if err := etf.Encode(control, packetBuffer, encodingOptions); err != nil {
		lib.ReleaseBuffer(packetBuffer)
		return fmt.Errorf("unable to encode control message: %s", err)
	}
	if payload != nil {
		if err := etf.Encode(payload, packetBuffer, encodingOptions); err != nil {
			lib.ReleaseBuffer(packetBuffer)
			return fmt.Errorf("unable to encode payload message: %s", err)
		}
	}

	lenMessage = packetBuffer.Len() - reserve
	lenAtomCache = 1
	startDataPosition -= lenAtomCache
	packetBuffer.B[startDataPosition] = byte(0) // empty cache

	for {
		// 4 (packet len) + 1 (dist header: 131) + 1 (dist header: protoDistMessage) + lenAtomCache
		lenPacket = 1 + 1 + lenAtomCache + lenMessage
		if fragmentationEnabled == false || lenMessage < c.fragment_unit {
			// send as a single packet
			startDataPosition -= 1
			packetBuffer.B[startDataPosition] = protoDistMessage // 68

			// 4 (packet len) + 1 (protoDist)
			startDataPosition -= 4 + 1

			binary.BigEndian.PutUint32(packetBuffer.B[startDataPosition:], uint32(lenPacket))
			packetBuffer.B[startDataPosition+4] = protoDist // 131

			bytesOut, err := c.flusher.Write(packetBuffer.B[startDataPosition:])
			if err != nil {
				return err
			}
			atomic.AddUint64(&c.bytesOut, uint64(bytesOut))
			atomic.AddUint64(&c.messagesOut, 1)
			break
		}

		// Message should be fragmented

		// https://erlang.org/doc/apps/erts/erl_ext_dist.html#distribution-header-for-fragmented-messages
		// "The entire atom cache and control message has to be part of the starting fragment"
		sequenceID := uint64(atomic.AddInt64(&c.fragment_id, 1))
		numFragments := lenMessage/c.fragment_unit + 1

		// 1 (dist header: 131) + 1 (dist header: protoDistFragment) + 8 (sequenceID) + 8 (fragmentID) + ...
		lenPacket = 1 + 1 + 8 + 8 + lenAtomCache + c.fragment_unit

		// 4 (packet len) + 1 (dist header: 131) + 1 (dist header: protoDistFragment[Z]) + 8 (sequenceID) + 8 (fragmentID)
		startDataPosition -= 22

		packetBuffer.B[startDataPosition+5] = protoDistFragment1 // 69

		binary.BigEndian.PutUint64(packetBuffer.B[startDataPosition+6:], uint64(sequenceID))
		binary.BigEndian.PutUint64(packetBuffer.B[startDataPosition+14:], uint64(numFragments))

		binary.BigEndian.PutUint32(packetBuffer.B[startDataPosition:], uint32(lenPacket))
		packetBuffer.B[startDataPosition+4] = protoDist // 131
		bytesOut, err := c.flusher.Write(packetBuffer.B[startDataPosition : startDataPosition+4+lenPacket])
		if err != nil {
			return err
		}
		atomic.AddUint64(&c.bytesOut, uint64(bytesOut))
		startDataPosition += 4 + lenPacket
		numFragments--

	nextFragment:

		if len(packetBuffer.B[startDataPosition:]) > c.fragment_unit {
			lenPacket = 1 + 1 + 8 + 8 + c.fragment_unit
			// reuse the previous 22 bytes for the next frame header
			startDataPosition -= 22

		} else {
			// the last one
			lenPacket = 1 + 1 + 8 + 8 + len(packetBuffer.B[startDataPosition:])
			startDataPosition -= 22
		}

		packetBuffer.B[startDataPosition+5] = protoDistFragmentN // 70

		binary.BigEndian.PutUint64(packetBuffer.B[startDataPosition+6:], uint64(sequenceID))
		binary.BigEndian.PutUint64(packetBuffer.B[startDataPosition+14:], uint64(numFragments))
		// send fragment
		binary.BigEndian.PutUint32(packetBuffer.B[startDataPosition:], uint32(lenPacket))
		packetBuffer.B[startDataPosition+4] = protoDist // 131
		bytesOut, err = c.flusher.Write(packetBuffer.B[startDataPosition : startDataPosition+4+lenPacket])
		if err != nil {
			return err
		}
		atomic.AddUint64(&c.bytesOut, uint64(bytesOut))
		startDataPosition += 4 + lenPacket
		numFragments--
		if numFragments > 0 {
			goto nextFragment
		}

		atomic.AddUint64(&c.messagesOut, 1)
		// done
		break
	}

	lib.ReleaseBuffer(packetBuffer)
	return nil
}

func (c *connection) waitResult(ref gen.Ref, ch chan messageResult) (result messageResult) {

	timer := lib.TakeTimer()
	defer lib.ReleaseTimer(timer)
	timer.Reset(time.Second * time.Duration(gen.DefaultRequestTimeout))

	select {
	case <-timer.C:
		result.Error = gen.ErrTimeout
	case result = <-ch:
	}

	c.requestsMutex.Lock()
	delete(c.requests, ref)
	c.requestsMutex.Unlock()

	return
}
