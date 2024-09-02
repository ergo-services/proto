package epmd

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"ergo.services/ergo/gen"

	"ergo.services/proto/erlang23/dist"
	"ergo.services/proto/erlang23/handshake"
)

// EPMD proto
// https://www.erlang.org/doc/apps/erts/erl_dist_protocol#epmd-protocol

type Options struct {
	Port           uint16
	EnableRouteTLS bool
	DisableServer  bool
}

func Create(options Options) gen.Registrar {
	if options.Port == 0 {
		options.Port = defaultEPMDPort
	}
	client := &client{
		options:    options,
		terminated: true,
	}

	return client
}

type client struct {
	node   gen.NodeRegistrar
	hidden bool
	route  gen.Route

	options Options

	server *server
	conn   net.Conn

	terminated bool
}

//
// gen.Resolver interface implementation
//

func (c *client) Resolve(nodename gen.Atom) ([]gen.Route, error) {
	if c.terminated {
		return nil, fmt.Errorf("EPMD client terminated")
	}

	n := strings.Split(string(nodename), "@")
	if len(n) != 2 || len(n[0]) == 0 || len(n[1]) == 0 {
		return nil, fmt.Errorf("incorrect FQDN node name (example: node@localhost)")
	}
	host := n[1]
	name := n[0]

	srv := c.server
	if srv != nil {
		c.node.Log().Trace("resolving %s using local EPMD server", name)
		routes, err := srv.resolve(name)
		if err != nil {
			return routes, err
		}
		routes[0].Host = host
		routes[0].HandshakeVersion = handshake.Version
		routes[0].ProtoVersion = dist.Version
		routes[0].TLS = c.options.EnableRouteTLS

		return routes, nil
	}

	dsn := net.JoinHostPort(host, strconv.Itoa(int(c.options.Port)))
	c.node.Log().Trace("resolving %s using EPMD %s", nodename, dsn)
	conn, err := net.Dial("tcp", net.JoinHostPort(n[1], fmt.Sprintf("%d", c.options.Port)))
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	if err := c.sendPortPleaseReq(conn, name); err != nil {
		return nil, err
	}

	port, err := c.readPortResp(conn)
	if err != nil {
		return nil, err
	}
	route := gen.Route{
		Host:             host,
		Port:             port,
		HandshakeVersion: handshake.Version,
		ProtoVersion:     dist.Version,
		TLS:              c.options.EnableRouteTLS,
	}

	return []gen.Route{route}, nil
}

func (c *client) ResolveApplication(name gen.Atom) ([]gen.ApplicationRoute, error) {
	return nil, gen.ErrUnsupported
}
func (c *client) ResolveProxy(node gen.Atom) ([]gen.ProxyRoute, error) {
	return nil, gen.ErrUnsupported
}

//
// gen.Registrar interface implementation
//

func (c *client) Resolver() gen.Resolver {
	return c
}

func (c *client) RegisterProxy(to gen.Atom) error {
	return gen.ErrUnsupported
}
func (c *client) UnregisterProxy(to gen.Atom) error {
	return gen.ErrUnsupported
}
func (c *client) RegisterApplicationRoute(route gen.ApplicationRoute) error {
	return gen.ErrUnsupported
}
func (c *client) UnregisterApplicationRoute(name gen.Atom) error {
	return gen.ErrUnsupported
}
func (c *client) Nodes() ([]gen.Atom, error) {
	return nil, gen.ErrUnsupported
}
func (c *client) Config(items ...string) (map[string]any, error) {
	return nil, gen.ErrUnsupported
}
func (c *client) ConfigItem(item string) (any, error) {
	return nil, gen.ErrUnsupported
}
func (c *client) Event() (gen.Event, error) {
	return gen.Event{}, gen.ErrUnsupported
}
func (c *client) Info() gen.RegistrarInfo {
	info := gen.RegistrarInfo{
		EmbeddedServer: c.server != nil,
		Version:        c.Version(),
	}
	conn := c.conn
	if conn != nil {
		info.Server = conn.RemoteAddr().String()
		return info
	}
	if info.EmbeddedServer {
		info.Server = c.server.socket.Addr().String()
	}
	return info
}

//
// gen.RegistrarClient interface implementation
//

func (c *client) Register(node gen.NodeRegistrar, routes gen.RegisterRoutes) (gen.StaticRoutes, error) {
	var static gen.StaticRoutes

	c.node = node

	if len(routes.Routes) == 0 {
		// do nothing
		return static, nil
	}

	c.hidden = true            // always hidden
	c.route = routes.Routes[0] // use the first one only

	if c.terminated == false {
		return static, fmt.Errorf("already started")
	}

	rc, err := c.tryRegister()
	if err != nil {
		return static, err
	}

	if rc != nil {
		go c.serve(rc)
	}

	c.terminated = false
	return static, nil
}

func (c *client) Terminate() {
	if c.server != nil {
		c.node.Log().Trace("terminate EPMD server")
		// c.server.terminate()
	}
	if c.conn != nil {
		c.conn.Close()
	}

	c.terminated = true
	c.node.Log().Trace("EPMD client terminated")
}

func (c *client) Version() gen.Version {
	return Version
}

func (c *client) tryRegister() (net.Conn, error) {
	// use the name only
	s := strings.Split(string(c.node.Name()), "@")
	if len(s) != 2 {
		return nil, fmt.Errorf("incorrect FQDN node name (example: node@localhost)")
	}
	if len(s[0]) < 1 {
		return nil, fmt.Errorf("too short node name")
	}
	if len(s[1]) < 1 {
		return nil, fmt.Errorf("too short host name")
	}

	if c.options.DisableServer == false {
		c.server = tryStartServer(c.options.Port, c.node.Log())
		if c.server != nil {
			// local registrar is started
			regNode := registeredNode{
				port:   c.route.Port,
				hidden: true, // dont use public
				hi:     6,    // hi handshake version
				lo:     5,    // lo handshake version
			}

			c.server.registerNode(s[0], regNode)
			return nil, nil
		}
		c.node.Log().Trace("unable to start EPMD server, run as a client only")
	}

	dialer := net.Dialer{
		KeepAlive: defaultKeepAlive,
	}
	dsn := net.JoinHostPort("localhost", strconv.Itoa(int(c.options.Port)))
	conn, err := dialer.Dial("tcp", dsn)
	if err != nil {
		return nil, err
	}

	if err := c.sendAliveReq(conn, s[0], c.route.Port); err != nil {
		conn.Close()
		return nil, err
	}

	if err := c.readAliveResp(conn); err != nil {
		conn.Close()
		return nil, err
	}

	conn.SetReadDeadline(time.Time{})
	return conn, nil
}

func (c *client) serve(conn net.Conn) {
	var buf [16]byte
	c.conn = conn

	for {
		_, err := c.conn.Read(buf[:])
		if c.terminated {
			return
		}
		if err != io.EOF {
			continue
		}

		// disconnected
		c.node.Log().Warning("lost connection with the EPMD registrar")
		c.conn = nil

		// trying to reconnect
		for {
			if c.terminated {
				return
			}
			conn, err := c.tryRegister()
			if err != nil {
				c.node.Log().Error("unable to register node on the EPMD registrar (will retry in a second): %s", err)
				time.Sleep(time.Second)
				continue
			}

			if conn == nil {
				// use the local registrar server
				c.node.Log().Info("registered node on the local EPMD registrar")
				return
			}
			c.conn = conn
			c.node.Log().Info("registered node on the EPMD registrar")
			break
		}

	}
}
func (c *client) sendPortPleaseReq(conn net.Conn, name string) error {
	buflen := uint16(2 + len(name) + 1)
	buf := make([]byte, buflen)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(buf)-2))
	buf[2] = byte(epmdPortPleaseReq)
	copy(buf[3:buflen], name)
	_, err := conn.Write(buf)
	return err
}

func (c *client) readPortResp(conn net.Conn) (uint16, error) {
	var port uint16
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("reading from link - %s", err)
	}
	if n < 5 {
		return 0, gen.ErrMalformed
	}
	buf = buf[:n]
	if buf[0] != epmdPortResp {
		return 0, gen.ErrMalformed
	}
	if buf[1] > 0 {
		return 0, gen.ErrNameUnknown
	}

	port = binary.BigEndian.Uint16(buf[2:4])
	// TODO should we care of the rest data?
	return port, nil
}

func (c *client) sendAliveReq(conn net.Conn, name string, port uint16) error {
	buf := make([]byte, 2+14+len(name))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(buf)-2))
	buf[2] = byte(epmdAliveReq)
	binary.BigEndian.PutUint16(buf[3:5], port)
	// http://erlang.org/doc/reference_manual/distributed.html (section 13.5)
	// 77 — regular public node, 72 — hidden
	// We use a regular one
	if c.hidden {
		buf[5] = 72
	} else {
		buf[5] = 77
	}
	// Protocol TCP
	buf[6] = 0
	// HighestVersion
	binary.BigEndian.PutUint16(buf[7:9], 6)
	// LowestVersion
	binary.BigEndian.PutUint16(buf[9:11], 5)
	// length Node name
	l := len(name)
	binary.BigEndian.PutUint16(buf[11:13], uint16(l))
	// Node name
	offset := (13 + l)
	copy(buf[13:offset], []byte(name))
	// Send
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	return nil
}

func (c *client) readAliveResp(conn net.Conn) error {
	buf := make([]byte, 16)
	if _, err := conn.Read(buf); err != nil {
		return err
	}
	switch buf[0] {
	case epmdAliveResp, epmdAliveRespX:
	default:
		return fmt.Errorf("malformed EPMD response %v", buf)
	}
	if buf[1] != 0 {
		if buf[1] == 1 {
			return fmt.Errorf("can not register node %s, name is taken", c.node.Name())
		}
		return fmt.Errorf("can not register %s, code: %d", c.node.Name(), buf[1])
	}
	return nil
}
