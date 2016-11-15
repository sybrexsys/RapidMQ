package queue

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sybrexsys/RapidMQ/queue/internal/logging"
)

const (
	testStateOk = iota
	testStateOkBeforeError
	testStateErrorsBeforeOk
)

type nullWorkerTestUnit struct {
	id               WorkerID
	cnt              int
	delay            time.Duration
	state            int
	changeStateCount int
	isTimeOut        bool
}

type nullWorkerTestUnitFactory struct {
	id               WorkerID
	cnt              int
	delay            time.Duration
	state            int
	changeStateCount int
	isTimeOut        bool
}

func (n *nullWorkerTestUnitFactory) CreateWorker() Worker {
	return &nullWorkerTestUnit{
		id:               WorkerID(atomic.AddUint64((*uint64)(&n.id), 1) - 1),
		changeStateCount: n.changeStateCount,
		delay:            n.delay,
		state:            n.state,
		isTimeOut:        n.isTimeOut,
	}
}

func (n *nullWorkerTestUnitFactory) NeedTimeoutProcessing() bool {
	return n.isTimeOut
}

func (n *nullWorkerTestUnit) isOk() bool {
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

func (n *nullWorkerTestUnit) ProcessMessage(q *Queue, msg *Message, Next chan Worker) {
	time.Sleep(n.delay)
	ID := WorkerID(atomic.AddUint64((*uint64)(&n.id), 0))
	if !n.isTimeOut {
		q.Process(ID, n.isOk())
	}
	Next <- n
	q.log.Trace("[Q:%s] Worker (%d) ready for work", q.name, ID)
}

func (n *nullWorkerTestUnit) ProcessTimeout(q *Queue, Next chan Worker) {
	ID := WorkerID(atomic.AddUint64((*uint64)(&n.id), 0))
	q.Process(ID, n.isOk())
	Next <- n
	q.log.Trace("[Q:%s] Worker (%d) ready for work", q.name, ID)
}

func (n *nullWorkerTestUnit) GetID() WorkerID {
	return WorkerID(atomic.AddUint64((*uint64)(&n.id), 0))
}

func WorkOptions(t *testing.T, kk int64, opt *Options, factory WorkerFactory, withLoging bool) bool {
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

	q, err := CreateQueue("Test", TestFolder, log, factory, opt)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
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
	for range picker.C {
		d++
		if d == 500 {
			t.Errorf("Not finished in %s...\n", time.Since(start))
			isOk = false
			break
		}
		if q.Count() == 0 {
			break
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
	factory WorkerFactory
	options Options
	logging bool
}

func TestQueue(t *testing.T) {
	DefaultQueueOptionsWithOutTimeout := DefaultQueueOptions
	DefaultQueueOptionsWithOutTimeout.InputTimeOut = 0

	tests := []testOptions{
		{ // Timeout On
			kk:      20,
			options: DefaultQueueOptions,
			factory: &nullWorkerTestUnitFactory{
				isTimeOut: false,
				state:     testStateOk,
			},
			logging: true,
		},
		{ //
			kk:      20,
			options: DefaultQueueOptions,
			factory: &nullWorkerTestUnitFactory{
				isTimeOut:        false,
				state:            testStateOkBeforeError,
				changeStateCount: 20,
			},
			logging: true,
		},

		{ // Timeout Off
			kk:      20,
			options: DefaultQueueOptionsWithOutTimeout,
			factory: &nullWorkerTestUnitFactory{
				isTimeOut: false,
				state:     testStateOk,
			},
			logging: true,
		},
		{ //
			kk:      20,
			options: DefaultQueueOptionsWithOutTimeout,
			factory: &nullWorkerTestUnitFactory{
				isTimeOut:        false,
				state:            testStateOkBeforeError,
				changeStateCount: 20,
			},
			logging: true,
		},

		{ // Timeout On
			kk:      20,
			options: DefaultQueueOptions,
			factory: &nullWorkerTestUnitFactory{
				isTimeOut: true,
				state:     testStateOk,
			},
			logging: true,
		},
		{ //
			kk:      20,
			options: DefaultQueueOptions,
			factory: &nullWorkerTestUnitFactory{
				isTimeOut:        true,
				state:            testStateOkBeforeError,
				changeStateCount: 20,
			},
			logging: true,
		},
		{ // Timeout Off
			kk:      20,
			options: DefaultQueueOptionsWithOutTimeout,
			factory: &nullWorkerTestUnitFactory{
				isTimeOut: true,
				state:     testStateOk,
			},
			logging: true,
		},
		{ //
			kk:      20,
			options: DefaultQueueOptionsWithOutTimeout,
			factory: &nullWorkerTestUnitFactory{
				isTimeOut:        true,
				state:            testStateOkBeforeError,
				changeStateCount: 20,
			},
			logging: true,
		},

		{ //
			kk:      100,
			options: DefaultQueueOptions,
			factory: &nullWorkerTestUnitFactory{
				isTimeOut:        true,
				state:            testStateOkBeforeError,
				changeStateCount: 20,
			},
			logging: true,
		},

		{ //
			kk:      100,
			options: DefaultQueueOptionsWithOutTimeout,
			factory: &nullWorkerFactory{},
			logging: true,
		},
	}

	for _, k := range tests {
		if !WorkOptions(t, k.kk, &k.options, k.factory, k.logging) {
			break
		}

	}

}
