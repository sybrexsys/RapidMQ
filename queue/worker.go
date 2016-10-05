package queue

import "sync/atomic"

type WorkerID uint64

type QueueWorker interface {
	ProcessMessage(*Queue, *QueueItem, chan QueueWorker)
	ProcessTimeout(*Queue, chan QueueWorker)
	CreateClone() QueueWorker
	GetID() WorkerID
	NeedTimeoutProcessing() bool
}

type nullReader struct {
	id             WorkerID
	generateErrors bool
}

func (n *nullReader) ProcessMessage(q *Queue, msg *QueueItem, Next chan QueueWorker) {
	ID := WorkerID(atomic.AddUint64((*uint64)(&n.id), 0))
	q.Process(ID, true)
	Next <- n
	q.log.Trace("[Q:%s] Worker (%d) ready for work", q.name, ID)
}
func (n *nullReader) ProcessTimeout(q *Queue, Next chan QueueWorker) {
}

func (n *nullReader) CreateClone() QueueWorker {
	return &nullReader{
		id:             WorkerID(atomic.AddUint64((*uint64)(&n.id), 1) - 1),
		generateErrors: n.generateErrors,
	}
}

func (n *nullReader) GetID() WorkerID {
	return WorkerID(atomic.AddUint64((*uint64)(&n.id), 0))
}

func (n *nullReader) NeedTimeoutProcessing() bool {
	return false
}
