package epmd

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"ergo.services/ergo/gen"
)

type Options struct {
	Port          uint16
	DisableServer bool
}

func CreateClient(options Options) gen.RegistrarClient {
	if options.Port == 0 {
		options.Port = defaultRegistrarPort
	}
	client := &client{
		options:    options,
		terminated: true,
	}

	return client
}

type client struct {
	node gen.NodeRegistrar

	routes []gen.Route

	options Options

	server *server
	conn   net.Conn

	terminated bool
}

//
// gen.Resolver interface implementation
//

func (c *client) Resolve(name gen.Atom) ([]gen.Route, error) {
	if c.terminated {
		return nil, fmt.Errorf("EPMD client terminated")
	}

	srv := c.server
	if srv != nil {
		c.node.Log().Trace("resolving %s using local EPMD server", name)
		return srv.resolve(name, true)
	}

	host := name.Host()
	if host == "" {
		return nil, gen.ErrIncorrect
	}
	dsn := net.JoinHostPort(host, strconv.Itoa(int(c.options.Port)))
	c.node.Log().Trace("resolving %s using EPMD %s", name, dsn)
	conn, err := net.Dial("udp", dsn)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return nil, nil
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
func (c *client) RegisterApplication(route gen.ApplicationRoute) error {
	return gen.ErrUnsupported
}
func (c *client) UnregisterApplication(name gen.Atom, reason error) error {
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
	return gen.RegistrarInfo{
		LocalServer: c.server != nil,
		Version:     c.Version(),
	}
}

//
// gen.RegistrarClient interface implementation
//

func (c *client) Register(node gen.NodeRegistrar, routes gen.RegisterRoutes) (gen.StaticRoutes, error) {
	var static gen.StaticRoutes

	c.node = node
	c.routes = routes.Routes

	if c.terminated == false {
		return static, fmt.Errorf("already started")
	}

	if len(c.routes) == 0 {
		// hidden mode. do not register node
		return static, nil
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
		c.node.Log().Trace("terminate registrar server")
		// c.server.terminate()
	}
	if c.conn != nil {
		c.conn.Close()
	}

	c.terminated = true
	c.node.Log().Trace("registrar client terminated")
}

func (c *client) Version() gen.Version {
	return gen.Version{
		Name:    registrarName,
		Release: registrarRelease,
		License: gen.LicenseMIT,
	}
}

func (c *client) tryRegister() (net.Conn, error) {
	if c.options.DisableServer == false {
		c.server = tryStartServer(c.options.Port, c.node.Log())
		if c.server != nil {
			// local registrar is started
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
		c.node.Log().Warning("lost connection with the registrar")
		c.conn = nil

		// trying to reconnect
		for {
			if c.terminated {
				return
			}
			conn, err := c.tryRegister()
			if err != nil {
				c.node.Log().Error("unable to register node on the registrar: %s", err)
				time.Sleep(time.Second)
				continue
			}

			if conn == nil {
				// use the local registrar server
				c.node.Log().Info("registered node on the local registrar")
				return
			}
			c.conn = conn
			c.node.Log().Info("registered node on the registrar")
			break
		}

	}
}
