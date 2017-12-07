//Package queue contain methods and structures for managing of the message queue
package queue

//TODO: for empty list skip size of theindex file

import (
	"encoding/binary"
	"io"
	"runtime"
	"sync/atomic"
	"time"
)

// QueueItem is elementh of the queue
type QueueItem struct { // nolint
	idx     StorageIdx
	ID      StorageIdx
	Stream  io.ReadSeeker
	storage storageProcessing
}

//Queue is a base structure for managing of the messages
type Queue struct {
	name         string
	options      *Options
	workers      chan Worker
	tmpworkers   chan Worker
	log          Logging
	newMessage   chan struct{}
	stopEvent    chan struct{}
	stopedHandle chan struct{}
	storage      *fileStorage
	memory       *queueMemory
	factory      WorkerFactory
	inProcess    *inProcessingPerWorker
	total        int32
	totalWorkers int32
	lastTimeGC   time.Duration
}

type newMessageNotificator interface {
	newMessageNotification()
}

//CreateQueue is function than creates and inits internal states  :
func CreateQueue(Name, StoragePath string, Log Logging, Factory WorkerFactory, Options *Options) (*Queue, error) {
	if Factory == nil {
		Factory = &nullWorkerFactory{}
	}
	if Options == nil {
		Options = &DefaultQueueOptions
	}
	if Log == nil {
		z := nullLog(0)
		Log = z
	}
	Log.Info("[Q:%s] is created...", Name)

	tmp := &Queue{
		total:        0,
		workers:      make(chan Worker, Options.MaximumWorkersCount),
		stopedHandle: make(chan struct{}),
		newMessage:   make(chan struct{}, 1),
		log:          Log,
		options:      Options,
		factory:      Factory,
		name:         Name,
		stopEvent:    make(chan struct{}),
		lastTimeGC:   time.Since(startTime),
	}

	fs, err := createStorage(Name, StoragePath, Log, Options.StorageOptions, Options.InputTimeOut, tmp)
	if err != nil {
		Log.Error("[Q:%s] cannot store storage...", Name)
		return nil, err
	}

	tmp.storage = fs
	tmp.memory = createMemoryQueue(Name, Options.MaximumMessagesInQueue, Options.MaximumQueueMessagesSize,
		fs, Log, Options.InputTimeOut, tmp)

	if Factory.NeedTimeoutProcessing() {
		tmp.tmpworkers = make(chan Worker, Options.MaximumWorkersCount)
	}

	if Factory.CanCreateWorkers() {
		for i := uint16(0); i < Options.MinimunWorkersCount; i++ {
			newWorker, err := Factory.CreateWorker()
			if err != nil {
				tmp.log.Trace("[Q:%s] New worker was not created. Error:%s", tmp.name, err.Error())
			} else {
				tmp.workers <- newWorker
				tmp.log.Trace("[Q:%s] [W:%d] New worker was created.", tmp.name, newWorker.GetID())
			}
		}
		tmp.totalWorkers = int32(len(tmp.workers))
	} else {
		tmp.totalWorkers = 0
	}
	tmp.inProcess = createInProcessing(tmp, Options.MinimunWorkersCount, Options.MaximumMessagesPerWorker)

	Log.Trace("[Q:%s] Initial count of the workers is %d", Name, len(tmp.workers))
	Log.Info("[Q:%s] Was created successful", Name)

	if tmp.storage.Count() != 0 {
		Log.Trace("[Q:%s] Found unprocessed messages in storage...", Name)
	}
	Log.Info("[Q:%s] Start main loop", Name)
	go tmp.loop()
	return tmp, nil
}

func (q *Queue) newMessageNotification() {
	select {
	case q.newMessage <- struct{}{}:
	default:
	}
}

func (q *Queue) getOneItemFromStorage() (*QueueItem, error) {
	MemData, err := q.memory.Get()
	if err == nil {
		stream, err := bufToStream(MemData.buf)
		if err != nil {
			return nil, err
		}
		fw := &QueueItem{
			idx:     MemData.idx,
			ID:      MemData.idx,
			Stream:  stream,
			storage: q.memory,
		}
		return fw, nil
	}
	return q.storage.Get()
}

// Process must be called from the worker of the message. In depends
// of the `isOk` parameter either messages are deleting from the queue
// or are marking as faulty and again processing after some timeout
func (q *Queue) process(worker WorkerID, isOk bool) {
	q.log.Trace("[Q:%s] Receiver answer from worker (%d) [%v]", q.name, worker, isOk)
	q.inProcess.processList(worker, isOk)
}

func (q *Queue) dropWorker(worker WorkerID) {
	q.log.Trace("[Q:%s] [W:%d] Dropping worker", q.name, worker)
	q.inProcess.processList(worker, false)
	atomic.AddInt32(&q.totalWorkers, -1)
}

//Count returns the count of the messages in the queue
func (q *Queue) Count() uint64 {
	return q.storage.Count() + q.memory.Count()
}

// Insert appends the message into the queue. In depends of the timeout's option either is trying
// to write message to the disk or is trying to process this message in the memory and writing to the
// disk only if timeout is expired shortly. Returns false if aren't processing / writing of the message
// in the during of the timeout or has some problems with  writing to disk
func (q *Queue) Insert(buf []byte) bool {
	if q.options.InputTimeOut == 0 {
		return q.insert(buf, nil)
	}
	var timeoutch <-chan time.Time
	ch := make(chan bool, 1)
	go q.insert(buf, ch)
	timeoutch = time.NewTimer(q.options.InputTimeOut << 1).C
	for {
		select {
		case answer := <-ch:
			return answer
		case <-timeoutch:
			return false
		}
	}
}

// InsertFile appends file to queue. After processing content of the file if result of the execution of the worker is
// successful file will deleted.
func (q *Queue) InsertFile(fileName string) bool {
	var prefix [8]byte
	binary.LittleEndian.PutUint64(prefix[:], magicNumberIsFile)
	buf := []byte(string(prefix[:]) + fileName)
	if q.options.InputTimeOut == 0 {
		return q.insert(buf, nil)
	}
	var timeoutch <-chan time.Time
	ch := make(chan bool, 1)
	go q.insert(buf, ch)
	timeoutch = time.NewTimer(q.options.InputTimeOut << 1).C
	for {
		select {
		case answer := <-ch:
			return answer
		case <-timeoutch:
			return false
		}
	}
}

func (q *Queue) insert(buf []byte, ch chan bool) bool {
	if ch == nil {
		ID, err := q.storage.Put(buf)
		if err == nil {
			q.log.Trace("[Q:%s:%d] Stored to file storage", q.name, ID)
			q.newMessageNotification()
		} else {
			q.log.Error("[Q:%s:%d] Storing to storage with error result [%s] ", q.name, ID, err.Error())
		}
		return err == nil
	}
	if q.storage.Count() == 0 {
		if ID, err := q.memory.Put(buf, ch); err == nil {
			q.log.Trace("[Q:%s:%d] Stored to memory storage", q.name, ID)
			q.newMessageNotification()
			return true
		}
	}

	ID, err := q.storage.Put(buf)
	if err == nil {
		q.log.Trace("[Q:%s:%d] Stored to file storage", q.name, ID)
		q.newMessageNotification()
	} else {
		q.log.Error("[Q:%s:%d] Storing to storage with error result [%s] ", q.name, ID, err.Error())
	}
	ch <- err == nil
	return err == nil
}

func (q *Queue) errorProcessing() {
}

func (q *Queue) timeoutProcess() {

	Timeout := time.NewTimer(30 * time.Second).C
	// wait until all Workers finished its work
forloop:
	for {
		select {
		case <-Timeout:
			break forloop
		default:
			// If all Workers is in chanel then break this loop
			if len(q.workers) == int(q.totalWorkers) {
				break forloop
			}
			runtime.Gosched()
		}
	}

	if q.tmpworkers == nil {
		return
	}
	//for each worker we check on unfinished work  and if found we send to this
	// Worker request to timeout processing
	// We using temporary chanel becase me must detect processing of the all Workers
	for len(q.workers) > 0 {
		worker := <-q.workers
		if q.inProcess.messagesInProcess(worker.GetID()) > 0 {
			go func() {
				worker := worker
				r := worker.ProcessTimeout()
				switch r {
				case ProcessedSuccessful:
					q.process(worker.GetID(), true)
				case ProcessedWithError:
					q.process(worker.GetID(), false)
				case ProcessedKillWorker:
					q.dropWorker(worker.GetID())
					worker.Close()
					return
				default:
				}
				q.tmpworkers <- worker
				q.log.Trace("[Q:%s] [W:%d] Worker ready for work", q.name, worker.GetID())
			}()
		} else {
			q.tmpworkers <- worker
		}
	}

	Timeout = time.NewTimer(30 * time.Second).C
forloop2:
	for {
		select {
		case <-Timeout:
			break forloop2
		default:
			if len(q.tmpworkers) == int(q.totalWorkers) {
				break forloop2
			}
			runtime.Gosched()
		}
	}
	// Now all Warkers is processed and don't have any unprocessed messages
	// Because timeout and not present messages in the storage we decrement count of the Workers
	for len(q.tmpworkers) > int(q.options.MinimunWorkersCount) {
		worker := <-q.tmpworkers
		q.inProcess.delete(worker.GetID())
		q.totalWorkers--
	}
	q.workers, q.tmpworkers = q.tmpworkers, q.workers

}

func (q *Queue) loop() {
	var to <-chan time.Time
	MaxWorkers := q.options.MaximumWorkersCount
	AvailableWorker := q.workers
	Timer := time.NewTimer(time.Millisecond * 10000)
	AC := 0
	MC := 1
gofor:
	for {
		select {
		case <-q.stopEvent:
			break gofor
		case <-q.newMessage:
			if AvailableWorker == nil {
				//		q.log.Trace("[Q:%s]New message was received or timeout expired. Start reading messages", q.name)
				AvailableWorker = q.workers
			}
			if q.factory.CanCreateWorkers() && len(q.workers) == 0 && uint16(q.totalWorkers) < MaxWorkers {
				tmp, err := q.factory.CreateWorker()
				if err == nil {
					q.workers <- tmp
					q.totalWorkers++
					q.log.Trace("[Q:%s] [W:%d] New worker was created  Current count is %d ", q.name, tmp.GetID(), q.totalWorkers)
				} else {
					q.log.Error("[Q:%s] New worker was created with error %s ", q.name, err.Error())
				}
			}
			to = nil
		case worker := <-AvailableWorker:
			inProcessItem := q.inProcess.addToList(worker.GetID())
			if inProcessItem == nil {
				go func() {
					worker := worker
					r := worker.ProcessTimeout()
					switch r {
					case ProcessedSuccessful:
						q.process(worker.GetID(), true)
					case ProcessedWithError:
						q.process(worker.GetID(), false)
					case ProcessedKillWorker:
						q.dropWorker(worker.GetID())
						worker.Close()
						return
					default:
					}
					q.workers <- worker
					q.log.Trace("[Q:%s] [W:%d] Worker ready for work", q.name, worker.GetID())
				}()
				continue
			}
			item, err := q.getOneItemFromStorage()
			if err == nil {
				inProcessItem[0] = item
				q.log.Trace("[Q:%s] [W:%d] [M:%d] Loaded from %s and sent to worker", q.name, worker.GetID(), item.ID, item.storage.description())
				go func() {
					worker := worker
					r := worker.ProcessMessage(item)
					switch r {
					case ProcessedSuccessful:
						q.process(worker.GetID(), true)
					case ProcessedWithError:
						q.process(worker.GetID(), false)
					case ProcessedKillWorker:
						q.dropWorker(worker.GetID())
						worker.Close()
						return
					default:
					}
					q.workers <- worker
					q.log.Trace("[Q:%s] [W:%d] Worker ready for work", q.name, worker.GetID())
				}()
				MC = 1
				continue
			}
			q.inProcess.decrementList(worker.GetID())
			AvailableWorker = nil
			q.workers <- worker
			myerr, ok := err.(*queueError)
			timer := time.Millisecond * 10000
			if ok {
				switch myerr.ErrorType {
				case errorInDelay:
					if myerr.NextAvailable < timer {
						timer = myerr.NextAvailable
					}
					q.log.Trace("[Q:%s] Next messages will available in %s", q.name, myerr.NextAvailable.String())
				case errorNoMore:
					//	q.log.Trace("[Q:%s] No mo available messages", q.name)
				}
			} else {
				q.log.Trace("[Q:%s] Received answer from storage %s", q.name, err.Error())
				q.errorProcessing()
			}
			if q.tmpworkers != nil {
				q.timeoutProcess()
			}
			if !Timer.Stop() {
				select {
				case <-Timer.C:
				default:
				}
			}
			Timer.Reset(timer)
			to = Timer.C //time.After(timer)
		case <-to:
			if AvailableWorker == nil {
				AvailableWorker = q.workers
				AC++
				if AC >= MC {
					q.log.Trace("[Q:%s] Idle ", q.name)
					if MC == 1 {
						MC += 2
						go q.storage.garbageCollect()
						q.log.Trace("[Q:%s] GC was started", q.name)
					} else {
						MC += 3
					}
					AC = 0
				}
			}
		}
	}
	q.timeoutProcess()
	q.stopedHandle <- struct{}{}
}

func (q *Queue) close() {
	q.stopEvent <- struct{}{}
	<-q.stopedHandle
	close(q.workers)
	for w := range q.workers {
		w.Close()
	}
	q.factory.Close()
	q.memory.Close()
	q.storage.Close()

}

func (q *Queue) info() {
	q.storage.info()
}

// Close stops the handler of the messages, saves the messages located in
// the memory into the disk, closes all opened files.
func (q *Queue) Close() {
	q.log.Info("[Q:%s] is closed...", q.name)
	q.close()
	q.log.Info("[Q:%s] was closed...", q.name)
}
