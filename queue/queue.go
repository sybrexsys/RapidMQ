package queue

/*
TODO: for empty list skip size of theindex file
*/

import (
	"github.com/sybrexsys/RapidMQ/logging"
	"runtime"
	"sync"
	"time"
)

type QueueOptions struct {
	MinimunWorkersCount      uint16
	MaximumWorkersCount      uint16
	StorageOptions           *StorageOptions
	MaximumMessagesPerWorker uint16
	StoreAllRecordsToStorage bool
	InputTimeOut             time.Duration
	MaximumQueueMessagesSize int32
	MaximumMessagesInQueue   uint16
}

var DefaultQueueOptions = QueueOptions{
	MinimunWorkersCount:      4,
	MaximumWorkersCount:      32,
	StorageOptions:           nil,
	MaximumMessagesPerWorker: 2048,
	StoreAllRecordsToStorage: false,
	InputTimeOut:             5 * time.Second,
	MaximumMessagesInQueue:   2048,
	MaximumQueueMessagesSize: 16 * 1024 * 1024,
}

type QueueItem struct {
	idx     StorageIdx
	ID      StorageIdx
	Buffer  []byte
	storage storageProcessing
}

type Queue struct {
	sync.Mutex
	total        int32
	name         string
	options      *QueueOptions
	totalWorkers uint16
	workers      chan QueueWorker
	tmpworkers   chan QueueWorker
	log          logging.Logging
	newMessage   chan struct{}
	stopEvent    chan struct{}
	stopedHandle chan struct{}
	storage      *fileStorage
	memory       *queueMemory
	masterWorker QueueWorker
	inProcess    *inProcessingPerWorker
}

type newMessageNotificator interface {
	newMessageNotification()
}

func CreateQueue(Name, StoragePath string, Log logging.Logging, Reader QueueWorker, Options *QueueOptions) (*Queue, error) {
	if Reader == nil {
		Reader = &nullReader{}
	}
	if Options == nil {
		Options = &DefaultQueueOptions
	}
	if Log == nil {
		z := logging.NullLog(0)
		Log = z
	}
	Log.Info("[Q:%s] is created...", Name)

	tmp := &Queue{
		total:        0,
		workers:      make(chan QueueWorker, Options.MaximumWorkersCount),
		stopedHandle: make(chan struct{}),
		newMessage:   make(chan struct{}, 1),
		log:          Log,
		options:      Options,
		masterWorker: Reader,
		name:         Name,
		stopEvent:    make(chan struct{}),
	}

	fs, err := createStorage(Name, StoragePath, Log, Options.StorageOptions, Options.InputTimeOut, tmp)
	if err != nil {
		Log.Error("[Q:%s] cannot store storage...", Name)
		return nil, err
	}

	tmp.storage = fs
	tmp.memory = createMemoryQueue(Name, Options.MaximumMessagesInQueue, Options.MaximumQueueMessagesSize,
		fs, Log, Options.InputTimeOut, tmp)

	if Reader.NeedTimeoutProcessing() {
		tmp.tmpworkers = make(chan QueueWorker, Options.MaximumWorkersCount)
	}
	for i := uint16(0); i < Options.MinimunWorkersCount; i++ {
		newReader := Reader.CreateClone()
		tmp.workers <- newReader
		tmp.log.Trace("[Q:%s] New reader (%d) was created.", tmp.name, newReader.GetID())
	}
	tmp.totalWorkers = Options.MinimunWorkersCount
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
		fw := &QueueItem{
			idx:     MemData.idx,
			ID:      MemData.idx,
			Buffer:  MemData.buf,
			storage: q.memory,
		}
		return fw, nil
	}
	return q.storage.Get()
}

func (q *Queue) Process(worker WorkerID, isOk bool) {
	q.log.Trace("[Q:%s] Receiver answer from worker (%d) [%v]", q.name, worker, isOk)
	q.inProcess.processList(worker, isOk)
}

func (q *Queue) Count() uint64 {
	return q.storage.Count() + q.memory.Count()
}

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
			go worker.ProcessTimeout(q, q.tmpworkers)
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
	Timer := time.NewTimer(time.Millisecond * 5000)
gofor:
	for {
		select {
		case <-q.stopEvent:
			break gofor
		case <-q.newMessage:
			if AvailableWorker == nil {
				q.log.Trace("[Q:%s]New message was received or timeout expired. Start reading messages", q.name)
				AvailableWorker = q.workers
			}
			if len(q.workers) == 0 && q.totalWorkers < MaxWorkers {
				tmp := q.masterWorker.CreateClone()
				q.workers <- tmp
				q.totalWorkers++
				q.log.Trace("[Q:%s] New reader (%d) was created  Current count is %d ", q.name, tmp.GetID(), q.totalWorkers)
			}
			to = nil
		case worker := <-AvailableWorker:
			inProcessItem := q.inProcess.addToList(worker.GetID())
			if inProcessItem == nil {
				go worker.ProcessTimeout(q, q.workers)
				continue
			}
			item, err := q.getOneItemFromStorage()
			if err == nil {
				inProcessItem[0] = workeridx{
					idx:     item.idx,
					storage: item.storage,
					ID:      item.ID,
				}
				q.log.Trace("[Q:%s:%d] Loaded from %s and sent to worker (%d)", q.name, item.ID, item.storage.description(), worker.GetID())
				go worker.ProcessMessage(q, item, q.workers)
				continue
			}
			q.inProcess.decrementList(worker.GetID())
			AvailableWorker = nil
			q.workers <- worker
			myerr, ok := err.(*queueError)
			timer := time.Millisecond * 5000
			if ok {
				switch myerr.ErrorType {
				case errorInDelay:
					if myerr.NextAvailable < timer {
						timer = myerr.NextAvailable
					}
					q.log.Trace("[Q:%s] Next messages will available in %s", q.name, myerr.NextAvailable.String())
				case errorNoMore:
					q.log.Trace("[Q:%s] No mo available messages", q.name)
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
				q.log.Trace("[Q:%s] Timeout detected", q.name)
			}
		}
	}
	q.timeoutProcess()
	q.stopedHandle <- struct{}{}
}

func (q *Queue) close() {
	q.stopEvent <- struct{}{}
	<-q.stopedHandle
	q.memory.Close()
	q.storage.Close()

}

func (q *Queue) Info() {
	q.storage.info()
}

func (q *Queue) Close() {
	q.log.Info("[Q:%s] is closed...", q.name)
	q.close()
	q.log.Info("[Q:%s] was closed...", q.name)
}
