package queue

import (
	"sync"
)

type storageProcessing interface {
	FreeRecord(idx StorageIdx) error
	UnlockRecord(idx StorageIdx) error
	description() string
}

type workeridx struct {
	idx     StorageIdx
	ID      StorageIdx
	storage storageProcessing
}

type inProcessingList struct {
	list []workeridx
	cnt  uint16
}

type inProcessingPerWorker struct {
	sync.RWMutex
	maxPerWorker uint16
	workers      map[WorkerID]*inProcessingList
	q            *Queue
}

func createInProcessing(q *Queue, workerCount, maxPerWorker uint16) *inProcessingPerWorker {
	return &inProcessingPerWorker{
		maxPerWorker: maxPerWorker,
		workers:      make(map[WorkerID]*inProcessingList, workerCount),
		q:            q,
	}
}

func (ipw *inProcessingPerWorker) addToList(worker WorkerID) []workeridx {
	ipw.RLock()
	list, ok := ipw.workers[worker]
	ipw.RUnlock()
	if !ok {
		list = &inProcessingList{
			list: make([]workeridx, ipw.maxPerWorker),
		}
		ipw.Lock()
		ipw.workers[worker] = list
		ipw.Unlock()
	}
	if list.cnt == ipw.maxPerWorker {
		return nil
	}
	list.cnt++
	return list.list[list.cnt-1 : list.cnt]
}

func (ipw *inProcessingPerWorker) decrementList(worker WorkerID) {
	ipw.RLock()
	list, ok := ipw.workers[worker]
	ipw.RUnlock()
	if !ok {
		return
	}
	list.cnt--
}

func (ipw *inProcessingPerWorker) processList(worker WorkerID, isOk bool) {
	ipw.RLock()
	list, ok := ipw.workers[worker]
	ipw.RUnlock()
	if !ok {
		ipw.q.log.Trace("[Q:%s] !!!Not records for (%d)", ipw.q.name, worker)
		return
	}
	if list.cnt == 0 {
		ipw.q.log.Trace("[Q:%s] !!!!Not records for (%d)", ipw.q.name, worker)
	}
	for i := uint16(0); i < list.cnt; i++ {
		if isOk {
			ipw.q.log.Trace("[Q:%s:%d] Delete message from %s", ipw.q.name, list.list[i].ID, list.list[i].storage.description())
			list.list[i].storage.FreeRecord(list.list[i].idx)
		} else {
			ipw.q.log.Trace("[Q:%s:%d] Mark message in %s as faulty", ipw.q.name, list.list[i].ID, list.list[i].storage.description())
			list.list[i].storage.UnlockRecord(list.list[i].idx)
		}
	}
	list.cnt = 0
}

func (ipw *inProcessingPerWorker) messagesInProcess(worker WorkerID) uint16 {
	ipw.RLock()
	list, ok := ipw.workers[worker]
	ipw.RUnlock()
	if !ok {
		return 0
	}
	return list.cnt
}

func (ipw *inProcessingPerWorker) delete(worker WorkerID) {
	ipw.Lock()
	delete(ipw.workers, worker)
	ipw.Unlock()
}
