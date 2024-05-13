package dist

import (
	"fmt"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/lib"
	"ergo.services/proto/erlang"
	"ergo.services/proto/erlang/etf"
)

// later  "ergo.services/proto/erlang/etf"
// "ergo.services/proto/erlang/handshake"

type dist struct {
	core               gen.Core
	fragmentation_unit int
}

type Options struct {
	FragmentationUnit int
}

func Create(options Options) gen.NetworkProto {
	if options.FragmentationUnit < defaultFragmentationUnit {
		options.FragmentationUnit = defaultFragmentationUnit
	}
	return &dist{
		fragmentation_unit: options.FragmentationUnit,
	}
}

// gen.NetworkProto implementation

func (d *dist) NewConnection(core gen.Core, result gen.HandshakeResult, log gen.Log) (gen.Connection, error) {

	opts, ok := result.Custom.(erlang.ConnectionOptions)
	if ok == false {
		return nil, fmt.Errorf("unsupported type in gen.HandshakeResult.Costom")
	}

	if result.PeerCreation == 0 {
		// seems it was Join handshake for the connection that was already terminated
		return nil, gen.ErrNotAllowed
	}

	log.Trace("create new connection with %s", result.Peer)
	conn := &connection{
		id:                  result.ConnectionID,
		creation:            time.Now().Unix(),
		core:                core,
		log:                 log,
		node_flags:          result.NodeFlags,
		node_maxmessagesize: result.NodeMaxMessageSize,
		node_erlang_flags:   opts.NodeFlags,

		handshakeVersion: result.HandshakeVersion,
		protoVersion:     d.Version(),

		peer:                result.Peer,
		peer_creation:       result.PeerCreation,
		peer_flags:          result.PeerFlags,
		peer_version:        result.PeerVersion,
		peer_maxmessagesize: result.PeerMaxMessageSize,
		peer_erlang_flags:   opts.PeerFlags,

		// requests: make(map[gen.Ref]chan MessageResult),

		cache:   etf.NewAtomCache(),
		mapping: etf.NewAtomMapping(),
	}

	// init recv queues. create 4 recv queues per connection
	// since the decoding is more costly comparing to the encoding
	for i := 0; i < 4; i++ {
		conn.recvQueues = append(conn.recvQueues, lib.NewQueueMPSC())
	}

	return conn, nil
}

func (d *dist) Serve(c gen.Connection, dial gen.NetworkDial) error {
	conn, valid := c.(*connection)
	if valid == false {
		return fmt.Errorf("internal DIST error: unsupported connection type")
	}
	conn.wait()
	return nil
}

func (d *dist) Version() gen.Version {
	return Version
}
