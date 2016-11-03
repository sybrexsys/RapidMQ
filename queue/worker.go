package queue

import "sync/atomic"

//WorkerFactory is interface for creating new workers
type WorkerFactory interface {
	// Creates new worker for this factory with unique ID
	CreateWorker() Worker
	// Returns true if possible used some messages in one action (for example,
	// collect large SQL script from lot of the small messages)
	NeedTimeoutProcessing() bool
}

//WorkerID is an identifier of the worker
type WorkerID uint64

//Worker is interface that allow to structure to processing outgoing message
type Worker interface {
	// Processes message that is stored in `*Message`.
	// After it the worker must call function `(*Queue).Process` with his unique identifier
	// and with result of the processing, also must be pushed himself into chanal `Worker`
	ProcessMessage(*Queue, *Message, chan Worker)

	// Processing of the event when available messages is absent
	// After it the worker must call function `(*Queue).Process` with his unique identifier and
	// with result of the processing, also must send himself into chanal `Worker`
	ProcessTimeout(*Queue, chan Worker)
	// Returns unique identifier of the worker
	GetID() WorkerID
}

type nullWorkerFactory struct {
	generateErrors bool
	nextID         WorkerID
}

type nullWorker struct {
	id             WorkerID
	generateErrors bool
}

func (n *nullWorker) ProcessMessage(q *Queue, msg *Message, Next chan Worker) {
	ID := n.id
	q.Process(ID, true)
	Next <- n
	q.log.Trace("[Q:%s] Worker (%d) ready for work", q.name, ID)
}
func (n *nullWorker) ProcessTimeout(q *Queue, Next chan Worker) {
}

func (n *nullWorker) GetID() WorkerID {
	return n.id
}

func (n *nullWorkerFactory) CreateWorker() Worker {
	return &nullWorker{
		id:             WorkerID(atomic.AddUint64((*uint64)(&n.nextID), 1) - 1),
		generateErrors: n.generateErrors,
	}
}

func (n *nullWorkerFactory) NeedTimeoutProcessing() bool {
	return false
}
