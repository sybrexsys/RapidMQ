package queue

import "sync/atomic"

//WorkerFactory is interface for creating new workers
type WorkerFactory interface {
	// Creates new worker for this factory with unique ID
	CreateWorker() (Worker, error)
	// Returns true if possible used some messages in one action (for example,
	// collect large SQL script from lot of the small messages)
	NeedTimeoutProcessing() bool
	CanCreateWorkers() bool
	Close()
}

//WorkerID is an identifier of the worker
type WorkerID uint64

// Results of the execution of the worker
const (
	ProcessedSuccessful = iota
	ProcessedWithError
	ProcessedWaitNext
	ProcessedKillWorker
)

//Worker is interface that allow to structure to processing outgoing message
type Worker interface {
	// Processes message that is stored in `*Message`.
	// After it the worker must call function `(*Queue).Process` with his unique identifier
	// and with result of the processing, also must be pushed himself into chanal `Worker`
	ProcessMessage(*QueueItem) int

	// Processing of the event when available messages is absent
	// After it the worker must call function `(*Queue).Process` with his unique identifier and
	// with result of the processing, also must send himself into chanal `Worker`
	ProcessTimeout() int
	// Returns unique identifier of the worker
	GetID() WorkerID
	// Close is called when queue is finishing work with worker. Here you can close connection to database or etc.
	Close()
}

type QueueWorkerFactory interface { // nolint
	CreateWorker() (Worker, error)
	NeedTimeoutProcessing() bool
	CanCreateWorkers() bool
	Close()
}

type nullWorker struct {
	id             WorkerID
	generateErrors bool
}

type nullWorkerFactory struct {
	generateErrors bool
	nextID         WorkerID
}

func (n *nullWorker) ProcessMessage(msg *QueueItem) int {
	return ProcessedSuccessful
}

func (n *nullWorker) ProcessTimeout() int {
	return ProcessedSuccessful
}

func (n *nullWorker) GetID() WorkerID {
	return n.id
}

func (n *nullWorker) Close() {
}

func (n *nullWorkerFactory) NeedTimeoutProcessing() bool {
	return false
}

func (n *nullWorkerFactory) CreateWorker() (Worker, error) {
	return &nullWorker{
		id:             WorkerID(atomic.AddUint64((*uint64)(&n.nextID), 1) - 1),
		generateErrors: n.generateErrors,
	}, nil
}

func (n *nullWorkerFactory) CanCreateWorkers() bool {
	return true
}

func (n *nullWorkerFactory) Close() {}

type saveMessagesFactory struct{}

func (n *saveMessagesFactory) NeedTimeoutProcessing() bool {
	return false
}

func (n *saveMessagesFactory) CreateWorker() (Worker, error) {
	return nil, nil
}

func (n *saveMessagesFactory) CanCreateWorkers() bool {
	return false
}

func (n *saveMessagesFactory) Close() {}
