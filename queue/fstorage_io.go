package queue

import (
	"os"
	"runtime"
	"sync"
	"time"
)

//import "fmt"

const (
	handleInAction = iota
	handleInChanel
)

type fileWrite struct {
	*os.File
	FileIdx StorageIdx
}

type filequeue struct {
	sync.RWMutex
	memMutex sync.Mutex
	storage  *fileStorage
	total    int
	state    map[StorageIdx]int
	toOut    chan *fileWrite
}

func createIOQueue(storage *fileStorage) *filequeue {
	return &filequeue{
		state:   make(map[StorageIdx]int, 16),
		toOut:   make(chan *fileWrite, storage.options.MaxOneTimeOpenedFiles),
		storage: storage,
	}
}

func (fq *filequeue) getHandle(recordSize uint32, idx StorageIdx, timeout time.Duration) (*fileWrite, error) {
	fq.RLock()
	defer fq.RUnlock()
	var to <-chan time.Time
	if timeout != 0 {
		to = time.NewTimer(timeout).C
	}
	for {
		select {
		case wrk := <-fq.toOut:
			if wrk.File == nil {
				continue
			}
			offset, err := wrk.Seek(0, 1)
			if err == nil && offset+int64(recordSize) < fq.storage.options.MaxDataFileSize {
				fq.memMutex.Lock()
				fq.state[wrk.FileIdx] = handleInAction
				fq.memMutex.Unlock()
				return wrk, nil
			}
			fq.storage.log.Trace("[FSQ:%s] Datafile {%d} is oversize. Will be closed...", fq.storage.name, idx)
			fq.memMutex.Lock()
			delete(fq.state, wrk.FileIdx)
			fq.memMutex.Unlock()
			wrk.Close()
		case <-to:
			return nil, &queueError{ErrorType: errorTimeOut}
		default:
			fq.memMutex.Lock()
			if len(fq.state) < int(fq.storage.options.MaxOneTimeOpenedFiles) {
				datFileName := fq.storage.folder + dataFileNameByID(idx)
				file, err := os.Create(datFileName)
				if err != nil {
					fq.storage.log.Error("[FSQ:%s] Cannot create datafile {%d} Error: %s", fq.storage.name, idx, err.Error())
					return nil, err
				}
				fq.storage.log.Trace("[FSQ:%s] Create datafile {%d}", fq.storage.name, idx)
				saveDataFileHeader(file)
				tmp := &fileWrite{
					File:    file,
					FileIdx: idx,
				}
				fq.state[idx] = handleInAction
				fq.memMutex.Unlock()
				return tmp, nil
			}
			fq.memMutex.Unlock()
			runtime.Gosched()
		}
	}
}

func (fq *filequeue) putHandle(handle *fileWrite, err error) {
	fq.memMutex.Lock()
	defer fq.memMutex.Unlock()
	fq.storage.log.Error("[FSQ:%s] Received {%d}", fq.storage.name, handle.FileIdx)
	if err == nil {
		fq.toOut <- handle
	}
	if err == nil && handle.File != nil {
		fq.state[handle.FileIdx] = handleInChanel
	} else {
		delete(fq.state, handle.FileIdx)
		if handle.File != nil {
			handle.Close()
		}
	}

}

func (fq *filequeue) canClear(idx StorageIdx) bool {
	fq.memMutex.Lock()
	defer fq.memMutex.Unlock()
	fq.storage.log.Error("[FSQ:%s] Received reques to delete {%d}", fq.storage.name, idx)
	if _, ok := fq.state[idx]; !ok {
		fq.storage.log.Error("[FSQ:%s] Not information about this file {%d}", fq.storage.name, idx)
		return true
	}
	return false
}

func (fq *filequeue) free() {
	fq.memMutex.Lock()
	defer fq.memMutex.Unlock()
	close(fq.toOut)
	for wrk := range fq.toOut {
		wrk.Close()
	}
	fq.state = nil
}
