package queue

import (
	"io"
	"math/rand"
	"os"
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

func (n *nullWorkerTestUnitFactory) CreateWorker() (Worker, error) {
	return &nullWorkerTestUnit{
		id:               WorkerID(atomic.AddUint64((*uint64)(&n.id), 1) - 1),
		changeStateCount: n.changeStateCount,
		delay:            n.delay,
		state:            n.state,
		isTimeOut:        n.isTimeOut,
	}, nil
}

func (n *nullWorkerTestUnitFactory) NeedTimeoutProcessing() bool {
	return n.isTimeOut
}

func (n *nullWorkerTestUnitFactory) CanCreateWorkers() bool {
	return true
}

func (n *nullWorkerTestUnitFactory) Close() {
}

func (n *nullWorkerTestUnit) Close() {
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

func (n *nullWorkerTestUnit) ProcessMessage(msg *QueueItem) int {
	time.Sleep(n.delay)
	if !n.isTimeOut {
		if n.isOk() {
			return ProcessedSuccessful
		}
		return ProcessedWithError
	}
	return ProcessedWaitNext
}

func (n *nullWorkerTestUnit) ProcessTimeout() int {
	if n.isOk() {
		return ProcessedSuccessful
	}
	return ProcessedWithError
}

func (n *nullWorkerTestUnit) GetID() WorkerID {
	return WorkerID(atomic.AddUint64((*uint64)(&n.id), 0))
}

func WorkOptions(t *testing.T, Step int, kk int64, opt *Options, factory WorkerFactory, withLoging bool) bool {
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
	for range picker.C {
		d++
		if d == 500 {
			t.Errorf("Step %v Not finished in %s...\n", Step, time.Since(start))
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

	for s, k := range tests {
		if !WorkOptions(t, s, k.kk, &k.options, k.factory, k.logging) {
			break
		}

	}

}

const fileCount = 50

type z struct {
	i int
}

func BenchmarkMapInside(b *testing.B) {
	RecCount := b.N
	m := make(map[StorageIdx]struct{}, fileCount)
	for i := StorageIdx(0); i < fileCount; i++ {
		m[i] = struct{}{}
	}
	mem := make([]StorageIdx, RecCount)
	for k := 0; k < RecCount; k++ {
		mem[k] = StorageIdx(rand.Int31n(fileCount))
	}

	check := make(map[StorageIdx]*z)
	for kk := range m {
		check[kk] = &z{i: 0}
	}
	for k := 0; k < RecCount; k++ {
		idx := mem[k]
		op := check[idx]
		op.i++
	}
}

func BenchmarkMapOutside(b *testing.B) {
	RecCount := b.N
	m := make(map[StorageIdx]struct{}, fileCount)
	for i := StorageIdx(0); i < fileCount; i++ {
		m[i] = struct{}{}
	}
	mem := make([]StorageIdx, RecCount)
	for k := 0; k < RecCount; k++ {
		mem[k] = StorageIdx(rand.Int31n(fileCount))
	}

	check := make(map[StorageIdx]*z)
	for kk := range m {
		check[kk] = &z{i: 0}
	}
	for kk := range check {
		cnt := 0
		for k := 0; k < RecCount; k++ {
			if mem[k] == kk {
				cnt++
			}
		}
		check[kk].i = cnt
	}
}

func TestGCQueue(t *testing.T) {
	clearTestFolder()
	log, err := logging.CreateLog(logFolder+"logfile.log", 1024*1024*200, 255)
	if err != nil {
		t.Fatalf("Cannot create logging file: %s", err)
	}
	q, err := CreateQueue("Test", TestFolder, log, &nullWorkerFactory{}, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	tmp := make([]byte, rand.Intn(0x3fff))
	saved := q.Insert(tmp)
	if !saved {
		t.Fatalf("Cannot insert date")
	}
	time.Sleep(11 * time.Second)
	_, err1 := os.Stat(TestFolder + "stg00000.dat")
	if err1 == nil {
		t.Fatalf("Dat file must me deleted")
	}
	q.Close()
	if log != nil {
		log.Close()
	}
}

type TestWorker struct {
	id WorkerID
}

type TestWorkerFactory struct {
	id WorkerID
}

func (n *TestWorkerFactory) CreateWorker() (Worker, error) {
	return &TestWorker{
		id: WorkerID(atomic.AddUint64((*uint64)(&n.id), 1) - 1),
	}, nil
}

func (n *TestWorkerFactory) CanCreateWorkers() bool {
	return true
}

func (n *TestWorkerFactory) NeedTimeoutProcessing() bool {
	return false
}

func (n *TestWorkerFactory) Close() {
}

func (n *TestWorker) ProcessMessage(msg *QueueItem) int {
	start, _ := msg.Stream.Seek(0, io.SeekCurrent)
	size, _ := msg.Stream.Seek(0, io.SeekEnd)
	size -= start
	msg.Stream.Seek(start, io.SeekStart)
	buf := make([]byte, size)
	msg.Stream.Read(buf)
	if string(buf) != "error" {
		return ProcessedSuccessful
	}
	return ProcessedWithError
}

func (n *TestWorker) ProcessTimeout() int {
	return ProcessedSuccessful
}

func (n *TestWorker) GetID() WorkerID {
	return n.id
}

func (n *TestWorker) Close() {
}

func TestErrorOnChangeFromMemoryToDisk(t *testing.T) {
	clearTestFolder()
	log, err := logging.CreateLog(logFolder+"logfile.log", 1024*1024*200, 255)
	if err != nil {
		t.Fatalf("Cannot create logging file: %s", err)
	}
	opt := DefaultQueueOptions
	opt.InputTimeOut = 0
	q, err := CreateQueue("Test", TestFolder, log, &TestWorkerFactory{}, &opt)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}

	tmp := make([]byte, 50000)
	for i := 0; i < 2; i++ {
		saved := q.Insert(tmp)
		if !saved {
			t.Fatalf("Cannot insert date")
		}
	}
	saved := q.Insert([]byte("error"))
	if !saved {
		t.Fatalf("Cannot insert date")
	}
	for i := 0; i < 2; i++ {
		saved := q.Insert(tmp)
		if !saved {
			t.Fatalf("Cannot insert date")
		}
	}
	time.Sleep(2000 * time.Millisecond)
	q.Close()
	t.Log("Last\n")
	log.Close()

}
