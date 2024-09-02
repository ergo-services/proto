package epmd

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/lib"
)

// EPMD proto
// https://www.erlang.org/doc/apps/erts/erl_dist_protocol#epmd-protocol

type registeredNode struct {
	conn   net.Conn
	port   uint16
	hidden bool
	hi     uint16
	lo     uint16
	extra  []byte
}
type server struct {
	port       uint16
	socket     net.Listener
	nodes      lib.Map[string, registeredNode]
	terminated bool
	log        gen.Log
}

func tryStartServer(port uint16, log gen.Log) *server {
	addressReg := fmt.Sprintf(":%d", port)
	socket, err := net.Listen("tcp", addressReg)
	if err != nil {
		// might be already taken. dont care
		return nil
	}

	srv := &server{
		socket: socket,
		log:    log,
		port:   port,
	}
	go srv.serveRegister()

	srv.log.Trace("(registrar) EPMD server started on tcp://%s ", socket.Addr())
	return srv
}

func (s *server) resolve(name string) ([]gen.Route, error) {
	var routes []gen.Route
	node, found := s.nodes.Load(name)
	if found == false {
		return routes, gen.ErrNoRoute
	}
	route := gen.Route{
		Port: node.port,
	}
	routes = append(routes, route)
	return routes, nil
}

func (s *server) serveRegister() {
	for {
		if s.terminated {
			return
		}
		conn, err := s.socket.Accept()
		if err != nil {
			return
		}

		go s.serveConn(conn)
	}
}

func (s *server) serveConn(conn net.Conn) {
	var name string
	var registration bool // this link is a registration
	defer func() {
		if registration {
			s.unregisterNode(name)
		}
		conn.Close()
	}()

	s.log.Trace("(registrar) EPMD server got connection from %s", conn.RemoteAddr())

	conn.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 4096) // must be enough for the register packet

	for {
		n, err := conn.Read(buf)
		if err != nil {
			s.log.Trace("(registrar) EPMD server got error %s: %s", conn.RemoteAddr(), err)
			return
		}

		if len(buf) < 3 {
			s.log.Error("(registrar) EPMD malformed package from %s: %#v", conn.RemoteAddr(), buf)
			return
		}

		switch buf[2] {
		case epmdAliveReq:
			nameNode, regNode, err := s.readAliveReq(buf[3:n])
			if err != nil {
				// send error and close connection
				s.sendAliveResp(conn, 1)
				return
			}

			// register node
			if err := s.registerNode(nameNode, regNode); err != nil {
				s.log.Error("(registrar) EPMD unable to register node: %s", err)
				s.sendAliveResp(conn, 1)
				return
			}

			// send alive response
			if err := s.sendAliveResp(conn, 0); err != nil {
				return
			}

			// enable keep alive on this connection
			if tcp, ok := conn.(*net.TCPConn); ok {
				tcp.SetKeepAlive(true)
				tcp.SetKeepAlivePeriod(5 * time.Second)
				tcp.SetNoDelay(true)
			}

			conn.SetReadDeadline(time.Time{})
			registration = true
			name = nameNode
			continue

		case epmdPortPleaseReq:
			requestedName := string(buf[3:n])
			node, exist := s.nodes.Load(requestedName)

			if exist == false {
				s.log.Trace("(registrar) EPMD requested name %q is not found", requestedName)
				conn.Write([]byte{epmdPortResp, 1})
				return
			}

			s.sendPortPleaseResp(conn, requestedName, node)
			return

		case epmdNamesReq:
			s.sendNamesResp(conn, buf[3:n])
			return
		}

		s.log.Error("(registrar) EPMD unknown request: %v", buf[2])
		return
	}
}

func (s *server) registerNode(name string, node registeredNode) error {
	if _, exist := s.nodes.LoadOrStore(name, node); exist {
		return gen.ErrTaken
	}
	s.log.Trace("(registrar) EPMD registered node %s on port %d", name, node.port)
	return nil
}

func (s *server) unregisterNode(name string) {
	if s.terminated {
		return
	}
	s.nodes.Delete(name)
	s.log.Trace("(registrar) EPMD unregistered node %s", name)
}

func (s *server) terminate() {
	s.terminated = true
	s.socket.Close()
	s.nodes.Range(func(_ string, v registeredNode) bool {
		if v.conn != nil {
			v.conn.Close()
		}
		return true
	})

	s.log.Trace("(registrar) EPMD server terminated")
}

func (s *server) readAliveReq(req []byte) (string, registeredNode, error) {
	if len(req) < 10 {
		return "", registeredNode{}, fmt.Errorf("malformed request %v", req)
	}
	// Name length
	l := binary.BigEndian.Uint16(req[8:10])
	// Name
	name := string(req[10 : 10+l])
	// Hidden
	hidden := false
	if req[2] == 72 {
		hidden = true
	}
	// node
	node := registeredNode{
		port:   binary.BigEndian.Uint16(req[0:2]),
		hidden: hidden,
		hi:     binary.BigEndian.Uint16(req[4:6]),
		lo:     binary.BigEndian.Uint16(req[6:8]),
		extra:  req[10+l:],
	}

	return name, node, nil
}

func (s *server) sendAliveResp(conn net.Conn, code int) error {
	buf := make([]byte, 6)
	buf[0] = epmdAliveRespX
	buf[1] = byte(code)

	now := uint32(time.Now().Unix())
	binary.BigEndian.PutUint32(buf[2:], now)
	_, err := conn.Write(buf)
	return err
}

func (s *server) sendNamesResp(conn net.Conn, req []byte) {
	var str strings.Builder
	var buf [4]byte

	binary.BigEndian.PutUint32(buf[0:4], uint32(s.port))
	str.WriteString(string(buf[0:]))

	s.nodes.Range(func(name string, node registeredNode) bool {
		// io:format("name ~ts at port ~p~n", [NodeName, Port]).
		s := fmt.Sprintf("name %s at port %d\n", name, node.port)
		str.WriteString(s)

		return true
	})
	conn.Write([]byte(str.String()))
	return
}

func (s *server) sendPortPleaseResp(conn net.Conn, name string, node registeredNode) {
	buf := make([]byte, 12+len(name)+2+len(node.extra))
	buf[0] = epmdPortResp

	// Result 0
	buf[1] = 0
	// Port
	binary.BigEndian.PutUint16(buf[2:4], uint16(node.port))
	// Hidden
	if node.hidden {
		buf[4] = 72
	} else {
		buf[4] = 77
	}
	// Protocol TCP
	buf[5] = 0
	// Highest version
	binary.BigEndian.PutUint16(buf[6:8], uint16(node.hi))
	// Lowest version
	binary.BigEndian.PutUint16(buf[8:10], uint16(node.lo))
	// Name
	binary.BigEndian.PutUint16(buf[10:12], uint16(len(name)))
	offset := 12 + len(name)
	copy(buf[12:offset], name)
	// Extra
	l := len(node.extra)
	copy(buf[offset:offset+l], node.extra)
	// send
	conn.Write(buf)
}
