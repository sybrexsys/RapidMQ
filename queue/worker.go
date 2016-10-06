package queue

import "sync/atomic"

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

	// Main worker (that was sent as parameter when created queue) must have possibility to make
	// clone of himself. This clone must perform same processing as his parent. The identifier of
	// the clone must be unique
	CreateClone() Worker

	// Returns unique identifier of the worker
	GetID() WorkerID

	// Returns true if possible used some messages in one action (for example,
	// collect large SQL script from lot of the small messages)
	NeedTimeoutProcessing() bool
}

type nullReader struct {
	id             WorkerID
	generateErrors bool
}

func (n *nullReader) ProcessMessage(q *Queue, msg *Message, Next chan Worker) {
	ID := WorkerID(atomic.AddUint64((*uint64)(&n.id), 0))
	q.Process(ID, true)
	Next <- n
	q.log.Trace("[Q:%s] Worker (%d) ready for work", q.name, ID)
}
func (n *nullReader) ProcessTimeout(q *Queue, Next chan Worker) {
}

func (n *nullReader) CreateClone() Worker {
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
