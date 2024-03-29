package dist

import (
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/lib"
)

// later  "ergo.services/proto/erlang/etf"
// "ergo.services/proto/erlang/handshake"

type dist struct {
	core gen.Core
}

func Create() gen.NetworkProto {
	return &dist{}
}

// gen.NetworkProto implementation

func (d *dist) NewConnection(core gen.Core, result gen.HandshakeResult, log gen.Log) (gen.Connection, error) {

	// opts, ok := result.Custom.(handshake.ConnectionOptions)
	// if ok == false {
	// 	return nil, fmt.Errorf("HandshakeResult.Custom has unknown type")
	// }

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

		handshakeVersion: result.HandshakeVersion,
		protoVersion:     d.Version(),

		peer:                result.Peer,
		peer_creation:       result.PeerCreation,
		peer_flags:          result.PeerFlags,
		peer_version:        result.PeerVersion,
		peer_maxmessagesize: result.PeerMaxMessageSize,

		// requests: make(map[gen.Ref]chan MessageResult),
	}

	// init recv queues. create 4 recv queues per connection
	// since the decoding is more costly comparing to the encoding
	for i := 0; i < 4; i++ {
		conn.recvQueues = append(conn.recvQueues, lib.NewQueueMPSC())
	}

	return conn, nil
}

func (d *dist) Serve(c gen.Connection, dial gen.NetworkDial) error {
	conn := c.(*connection)
	conn.wait()
	return nil
}

func (d *dist) Version() gen.Version {
	return gen.Version{
		Name:    protoName,
		Release: protoRelease,
		License: gen.LicenseMIT,
	}
}
