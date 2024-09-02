package erlang23

import (
	"errors"
	"fmt"
	"runtime"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/lib"
	"ergo.services/proto/erlang23/etf"
)

// GenServerBehavior interface
type GenServerBehavior interface {
	gen.ProcessBehavior

	// Init invoked on a spawn GenServer for the initializing.
	Init(args ...any) error

	HandleInfo(message any) error
	HandleCast(message any) error
	HandleCall(from gen.PID, ref gen.Ref, request any) (any, error)
	Terminate(reason error)

	// HandleEvent invoked on an event message if this process got subscribed on
	// this event using gen.Process.LinkEvent or gen.Process.MonitorEvent
	HandleEvent(message gen.MessageEvent) error

	// HandleInspect invoked on the request made with gen.Process.Inspect(...)
	HandleInspect(from gen.PID, item ...string) map[string]string
}

// GenServer implementats ProcessBehavior interface and provides callbacks for
// - initialization
// - handling messages/requests.
// - termination
// All callbacks of the GenServerBehavior are optional for the implementation.
type GenServer struct {
	gen.Process

	behavior GenServerBehavior
	mailbox  gen.ProcessMailbox

	trap bool // trap exit
}

// SetTrapExit enables/disables the trap on exit requests sent by SendExit(...).
func (gs *GenServer) SetTrapExit(trap bool) {
	gs.trap = trap
}

// TrapExit returns whether the trap was enabled on this actor
func (gs *GenServer) TrapExit() bool {
	return gs.trap
}

// Caet send cast-message to Erlang-process
func (gs *GenServer) Cast(to any, message any) error {
	msg := etf.Tuple{
		gen.Atom("$gen_cast"),
		message,
	}
	return gs.Send(to, msg)
}

//
// ProcessBehavior implementation
//

// ProcessInit
func (gs *GenServer) ProcessInit(process gen.Process, args ...any) (rr error) {
	var ok bool

	if gs.behavior, ok = process.Behavior().(GenServerBehavior); ok == false {
		return errors.New("ProcessInit: not a GenServerBehavior")
	}

	if lib.Recover() {
		defer func() {
			if r := recover(); r != nil {
				pc, fn, line, _ := runtime.Caller(2)
				gs.Log().Panic("GenServer initialization failed. Panic reason: %#v at %s[%s:%d]",
					r, runtime.FuncForPC(pc).Name(), fn, line)
				rr = gen.TerminateReasonPanic
			}
		}()
	}

	gs.Process = process
	gs.mailbox = process.Mailbox()

	return gs.behavior.Init(args...)
}

func (gs *GenServer) ProcessRun() (rr error) {
	var message *gen.MailboxMessage

	if lib.Recover() {
		defer func() {
			if r := recover(); r != nil {
				pc, fn, line, _ := runtime.Caller(2)
				gs.Log().Panic("GenServer terminated. Panic reason: %#v at %s[%s:%d]",
					r, runtime.FuncForPC(pc).Name(), fn, line)
				rr = gen.TerminateReasonPanic
			}
		}()
	}

	for {
		if gs.State() != gen.ProcessStateRunning {
			// process was killed by the node.
			return gen.TerminateReasonKill
		}

		if message != nil {
			gen.ReleaseMailboxMessage(message)
			message = nil
		}

		for {
			// check queues
			msg, ok := gs.mailbox.Urgent.Pop()
			if ok {
				// got new urgent message. handle it
				message = msg.(*gen.MailboxMessage)
				break
			}

			msg, ok = gs.mailbox.System.Pop()
			if ok {
				// got new system message. handle it
				message = msg.(*gen.MailboxMessage)
				break
			}

			msg, ok = gs.mailbox.Main.Pop()
			if ok {
				// got new regular message. handle it
				message = msg.(*gen.MailboxMessage)
				break
			}

			msg, ok = gs.mailbox.Log.Pop()
			if ok {
				panic("GenServer process can not be a logger")
			}

			// no messages in the mailbox
			return nil
		}

	retry:
		switch message.Type {
		case gen.MailboxMessageTypeRegular:
			var reason error
			tuple, ok := message.Message.(etf.Tuple)
			if ok == false {
				reason = gs.behavior.HandleInfo(message.Message)
				if reason != nil {
					return reason
				}
				continue
			}

			if len(tuple) == 2 && tuple.Element(1) == gen.Atom("$gen_cast") {
				reason = gs.behavior.HandleCast(tuple.Element(2))
				if reason != nil {
					return reason
				}
				continue
			}

			if len(tuple) == 3 && tuple.Element(1) == gen.Atom("$gen_call") {
				// Due to lack of normal sync request/response in the DIST
				// proto we have to do all this magic below...
				tupleFrom, ok := tuple.Element(2).(etf.Tuple)
				if ok == false || len(tupleFrom) != 2 {
					reason = gs.behavior.HandleInfo(message.Message)
					if reason != nil {
						return reason
					}
					continue
				}

				from, ok := tupleFrom.Element(1).(gen.PID)
				if ok == false {
					reason = gs.behavior.HandleInfo(message.Message)
					if reason != nil {
						return reason
					}
					continue
				}

				if ref, ok := tupleFrom.Element(2).(gen.Ref); ok {
					request := tuple.Element(3)
					result, reason := gs.behavior.HandleCall(from, ref, request)
					if reason != nil {
						// if reason is "normal" and we got response - send it before termination
						if reason == gen.TerminateReasonNormal && result != nil {
							gs.SendResponse(message.From, ref, result)
						}
						return reason
					}

					if result == nil {
						// async handling of sync request. response could be sent
						// later, even by the other process
						continue
					}

					gs.SendResponse(message.From, ref, result)
					continue
				}

				// I have no words how worst Erlang's approach to get
				// rid of 'phantom'-messages (late replies).
				// But it is what it is.
				// Check if it was request with "alias"
				// etf.List[gen.Atom("alias"), gen.Ref]
				if list, ok := tupleFrom.Element(2).(etf.List); ok && len(list) == 2 {
					if ref, ok := list.Element(2).(gen.Ref); ok &&
						list.Element(1) == gen.Atom("alias") {

						// a little trick to support async reply with SendResponse.
						// the last ID is unused in Ergo so we can take some bits there
						// for the erlang support. set 4th bit as a flag which SendResponse
						// could rely on - whether to send reply to the PID or Alias.
						// (first 3 bits are taken already for keeping the length of IDs
						// in the erlnag's ref)
						ref.ID[2] |= (1 << 3)

						request := tuple.Element(3)
						result, reason := gs.behavior.HandleCall(from, ref, request)
						if reason != nil {
							// if reason is "normal" and we got response - send it before termination
							if reason == gen.TerminateReasonNormal && result != nil {
								gs.SendResponse(message.From, ref, result)
							}
							return reason
						}

						if result == nil {
							// async handling of sync request. response could be sent
							// later, even by the other process
							continue
						}

						gs.SendResponse(message.From, ref, result)
						continue
					}
				}
			}

			// handle as a regular message
			reason = gs.behavior.HandleInfo(message.Message)
			if reason != nil {
				return reason
			}
			continue

		case gen.MailboxMessageTypeRequest:
			var reason error
			var result any

			result, reason = gs.behavior.HandleCall(message.From, message.Ref, message.Message)

			if reason != nil {
				// if reason is "normal" and we got response - send it before termination
				if reason == gen.TerminateReasonNormal && result != nil {
					gs.SendResponse(message.From, message.Ref, result)
				}
				return reason
			}

			if result == nil {
				// async handling of sync request. response could be sent
				// later, even by the other process
				continue
			}

			gs.SendResponse(message.From, message.Ref, result)

		case gen.MailboxMessageTypeEvent:
			if reason := gs.behavior.HandleEvent(message.Message.(gen.MessageEvent)); reason != nil {
				return reason
			}

		case gen.MailboxMessageTypeExit:
			switch exit := message.Message.(type) {
			case gen.MessageExitPID:
				// trap exit signal if it wasn't send by parent
				// and TrapExit == true
				if gs.trap && message.From != gs.Parent() {
					message.Type = gen.MailboxMessageTypeRegular
					goto retry
				}
				return fmt.Errorf("%s: %w", exit.PID, exit.Reason)

			case gen.MessageExitProcessID:
				if gs.trap {
					message.Type = gen.MailboxMessageTypeRegular
					goto retry
				}
				return fmt.Errorf("%s: %w", exit.ProcessID, exit.Reason)

			case gen.MessageExitAlias:
				if gs.trap {
					message.Type = gen.MailboxMessageTypeRegular
					goto retry
				}
				return fmt.Errorf("%s: %w", exit.Alias, exit.Reason)

			case gen.MessageExitEvent:
				if gs.trap {
					message.Type = gen.MailboxMessageTypeRegular
					goto retry
				}
				return fmt.Errorf("%s: %w", exit.Event, exit.Reason)

			case gen.MessageExitNode:
				if gs.trap {
					message.Type = gen.MailboxMessageTypeRegular
					goto retry
				}
				return fmt.Errorf("%s: %w", exit.Name, gen.ErrNoConnection)

			default:
				panic(fmt.Sprintf("unknown exit message: %#v", exit))
			}

		case gen.MailboxMessageTypeInspect:
			result := gs.behavior.HandleInspect(message.From, message.Message.([]string)...)
			gs.SendResponse(message.From, message.Ref, result)
		}

	}
}
func (gs *GenServer) ProcessTerminate(reason error) {
	gs.behavior.Terminate(reason)
}

//
// default callbacks for GenServerBehavior interface
//

// Init
func (gs *GenServer) Init(args ...any) error {
	return nil
}

func (gs *GenServer) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	gs.Log().Warning("GenServer.HandleCall: unhandled request from %s", from)
	return gen.Atom("unhandled"), nil
}

func (gs *GenServer) HandleInfo(message any) error {
	gs.Log().Warning("GenServer.HandleInfo: unhandled message %v", message)
	return nil
}

func (gs *GenServer) HandleCast(message any) error {
	gs.Log().Warning("GenServer.HandleCast: unhandled message %v", message)
	return nil
}

func (gs *GenServer) HandleInspect(from gen.PID, item ...string) map[string]string {
	return nil
}

func (gs *GenServer) HandleEvent(message gen.MessageEvent) error {
	gs.Log().Warning("GenServer.HandleEvent: unhandled event message %#v", message)
	return nil
}

func (gs *GenServer) Terminate(reason error) {}
