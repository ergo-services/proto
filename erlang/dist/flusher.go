package dist

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ergo-services/ergo/lib"
)

var (
	// KeepAlive packet is just 4 bytes with zero value
	keepAlivePacket = []byte{0, 0, 0, 0}
	keepAlivePeriod = 15 * time.Second
)

type LinkWriterQueue struct {
	mpsc    lib.QueueMPSC
	socket  io.Writer
	running int32
}

func newLinkWriterQueue(socket io.Writer) *LinkWriterQueue {
	return &LinkWriterQueue{
		mpsc:   lib.NewQueueMPSC(),
		socket: socket,
	}
}

func newLinkWriter(q *LinkWriterQueue, latency time.Duration, ka bool) *linkWriter {
	lw := &linkWriter{
		queue:     q,
		buffer:    lib.TakeBuffer(),
		latency:   latency,
		keepAlive: ka,
	}

	lw.timer = time.AfterFunc(keepAlivePeriod, func() {

		lw.mutex.Lock()
		defer lw.mutex.Unlock()

		if lw.pending == false {
			if lw.keepAlive {
				// if we have no data in the buffer and enabled 'keepAlive'
				// send a KeepAlive packet
				b := lib.TakeBuffer()
				b.Append(keepAlivePacket)
				lw.queue.mpsc.Push(b)
				lw.runQueue()
				lw.timer.Reset(keepAlivePeriod)
			}
			return
		}

		lw.queue.mpsc.Push(lw.buffer)
		lw.buffer = lib.TakeBuffer()
		lw.pending = false
		lw.runQueue()
	})

	return lw
}

type linkWriter struct {
	queue  *LinkWriterQueue
	buffer *lib.Buffer

	mutex     sync.Mutex
	latency   time.Duration
	keepAlive bool

	timer   *time.Timer
	pending bool
}

func (lw *linkWriter) Write(b []byte) (int, error) {
	lw.mutex.Lock()
	defer lw.mutex.Unlock()

	lw.buffer.Append(b)
	lw.pending = true
	lw.timer.Reset(lw.latency)
	return len(b), nil
}

func (lw *linkWriter) runQueue() {
	if atomic.CompareAndSwapInt32(&lw.queue.running, 0, 1) == false {
		// already running
		return
	}
	go func() {
		for {
			item, ok := lw.queue.mpsc.Pop()
			if ok == false {
				// empty
				atomic.StoreInt32(&lw.queue.running, 0)
				return
			}
			buffer := item.(*lib.Buffer)
			l := buffer.Len()
			for {
				n, e := lw.queue.socket.Write(buffer.B)
				if e != nil {
					panic(e)
				}
				// check if something left
				l -= n
				if l > 0 {
					continue
				}

				lib.ReleaseBuffer(buffer)
				break
			}
		}
	}()
}

func (lw *linkWriter) Stop() {
	if lw.timer != nil {
		lw.timer.Stop()
	}
}
