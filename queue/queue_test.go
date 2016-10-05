package queue

import (
	"github.com/sybrexsys/RapidMQ/logging"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testStateOk = iota
	testStateOkBeforeError
	testStateErrorsBeforeOk
)

type nullReaderTestUnit struct {
	id               WorkerID
	cnt              int
	delay            time.Duration
	state            int
	changeStateCount int
	isTimeOut        bool
}

func (n *nullReaderTestUnit) CreateClone() QueueWorker {
	return &nullReaderTestUnit{
		id:               WorkerID(atomic.AddUint64((*uint64)(&n.id), 1) - 1),
		changeStateCount: n.changeStateCount,
		delay:            n.delay,
		state:            n.state,
		isTimeOut:        n.isTimeOut,
	}
}

func (n *nullReaderTestUnit) isOk() bool {
	switch n.state {
	case testStateOk:
		return true
	case testStateOkBeforeError:
		if n.cnt == n.changeStateCount {
			n.cnt = 0
			return false
		}
		n.cnt++
		return true

	case testStateErrorsBeforeOk:
		if n.cnt == n.changeStateCount {
			n.cnt = 0
			return true
		}
		n.cnt++
		return false
	}
	return true
}

func (n *nullReaderTestUnit) ProcessMessage(q *Queue, msg *QueueItem, Next chan QueueWorker) {
	time.Sleep(n.delay)
	ID := WorkerID(atomic.AddUint64((*uint64)(&n.id), 0))
	if !n.isTimeOut {
		q.Process(ID, n.isOk())
	}
	Next <- n
	q.log.Trace("[Q:%s] Worker (%d) ready for work", q.name, ID)
}

func (n *nullReaderTestUnit) ProcessTimeout(q *Queue, Next chan QueueWorker) {
	ID := WorkerID(atomic.AddUint64((*uint64)(&n.id), 0))
	q.Process(ID, n.isOk())
	Next <- n
	q.log.Trace("[Q:%s] Worker (%d) ready for work", q.name, ID)
}

func (n *nullReaderTestUnit) GetID() WorkerID {
	return WorkerID(atomic.AddUint64((*uint64)(&n.id), 0))
}

func (n *nullReaderTestUnit) NeedTimeoutProcessing() bool {
	return n.isTimeOut
}

func WorkOptions(t *testing.T, kk int64, opt *QueueOptions, worker QueueWorker, withLoging bool) bool {
	var (
		log *logging.Logger
		err error
	)
	isOk := true
	start := time.Now()
	clearTestFolder()
	if withLoging {
		log, err = logging.CreateLog(logFolder+"/logfile.log", 1024*1024*200, 255)
		if err != nil {
			t.Fatalf("Cannot create logging file: %s", err)
		}
	}

	q, err := CreateQueue("Test", TestFolder, log, worker, opt)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	totsize := uint64(0)
	tot := uint64(0)
	var m sync.Mutex
	var wg sync.WaitGroup
	for i := int64(0); i < kk; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			for j := int64(0); j < kk; j++ {
				tmp := make([]byte, rand.Intn(0x3fff))
				m.Lock()
				totsize += uint64(len(tmp))
				wrk := tot
				tot++
				m.Unlock()
				saved := q.Insert(tmp)
				if !saved {
					t.Fatalf("Cannot insert date:%d", wrk)
				}

			}
		}(i)
	}
	wg.Wait()
	picker := time.NewTicker(10 * time.Millisecond)
	d := 0
loop:
	for {
		select {
		case <-picker.C:
			d++
			if d == 500 {
				t.Errorf("Not finished in %s...\n", time.Since(start))
				isOk = false
				break loop
			}
			if q.Count() == 0 {
				break loop
			}
		}

	}
	q.Close()
	if log != nil {
		log.Close()
	}
	return isOk
}

type testOptions struct {
	kk      int64
	worker  QueueWorker
	options QueueOptions
	logging bool
}

func TestQueue(t *testing.T) {
	DefaultQueueOptionsWithOutTimeout := DefaultQueueOptions
	DefaultQueueOptionsWithOutTimeout.InputTimeOut = 0

	tests := []testOptions{
		{ // Timeout On
			kk:      20,
			options: DefaultQueueOptions,
			worker: &nullReaderTestUnit{
				isTimeOut: false,
				state:     testStateOk,
			},
			logging: true,
		},
		{ //
			kk:      20,
			options: DefaultQueueOptions,
			worker: &nullReaderTestUnit{
				isTimeOut:        false,
				state:            testStateOkBeforeError,
				changeStateCount: 20,
			},
			logging: true,
		},

		{ // Timeout Off
			kk:      20,
			options: DefaultQueueOptionsWithOutTimeout,
			worker: &nullReaderTestUnit{
				isTimeOut: false,
				state:     testStateOk,
			},
			logging: true,
		},
		{ //
			kk:      20,
			options: DefaultQueueOptionsWithOutTimeout,
			worker: &nullReaderTestUnit{
				isTimeOut:        false,
				state:            testStateOkBeforeError,
				changeStateCount: 20,
			},
			logging: true,
		},

		{ // Timeout On
			kk:      20,
			options: DefaultQueueOptions,
			worker: &nullReaderTestUnit{
				isTimeOut: true,
				state:     testStateOk,
			},
			logging: true,
		},
		{ //
			kk:      20,
			options: DefaultQueueOptions,
			worker: &nullReaderTestUnit{
				isTimeOut:        true,
				state:            testStateOkBeforeError,
				changeStateCount: 20,
			},
			logging: true,
		},
		{ // Timeout Off
			kk:      20,
			options: DefaultQueueOptionsWithOutTimeout,
			worker: &nullReaderTestUnit{
				isTimeOut: true,
				state:     testStateOk,
			},
			logging: true,
		},
		{ //
			kk:      20,
			options: DefaultQueueOptionsWithOutTimeout,
			worker: &nullReaderTestUnit{
				isTimeOut:        true,
				state:            testStateOkBeforeError,
				changeStateCount: 20,
			},
			logging: true,
		},

		{ //
			kk:      100,
			options: DefaultQueueOptions,
			worker: &nullReaderTestUnit{
				isTimeOut:        true,
				state:            testStateOkBeforeError,
				changeStateCount: 20,
			},
			logging: true,
		},

		{ //
			kk:      100,
			options: DefaultQueueOptionsWithOutTimeout,
			worker:  &nullReader{},
			logging: true,
		},
	}

	for _, k := range tests {
		if !WorkOptions(t, k.kk, &k.options, k.worker, k.logging) {
			break
		}

	}

}
