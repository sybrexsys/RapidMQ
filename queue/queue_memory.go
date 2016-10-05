package queue

import (
	"RapidMQ/logging"
	"sync"
	"time"
)

type queueMemoryItem struct {
	idx            StorageIdx
	ch             chan bool
	buf            []byte
	isBusy         bool
	filestorageidx StorageIdx
	timeOut        time.Duration
}

type queueMemory struct {
	sync.Mutex
	name    string
	idx     StorageIdx
	maxSize int32
	maxCnt  uint64
	cnt     uint64
	size    int32
	list    []*queueMemoryItem
	putter  storagePutter
	log     logging.Logging
	timeout time.Duration
	notify  newMessageNotificator
}

func createMemoryQueue(Name string, MaxCount uint16, MaxSize int32,
	Putter storagePutter, Log logging.Logging, TimeOut time.Duration, Notify newMessageNotificator) *queueMemory {
	if Log == nil {
		Log = logging.NullLog(0)
	}
	tmp := &queueMemory{
		name:    Name,
		list:    make([]*queueMemoryItem, MaxCount),
		maxCnt:  uint64(MaxCount),
		maxSize: MaxSize,
		putter:  Putter,
		log:     Log,
		timeout: TimeOut,
	}
	return tmp
}

func (mq *queueMemory) processTimeOut(curidx *int) {
	var (
		idx StorageIdx
		err error
	)
	if mq.list[*curidx].isBusy {
		if mq.list[*curidx].filestorageidx == InvalidIdx {
			idx, err = mq.putter.put(mq.list[*curidx].buf, putRecordAsInProcess)
		} else {
			return
		}
	} else {
		idx, err = mq.putter.put(mq.list[*curidx].buf, putRecordAsNew)
	}
	if err == nil && mq.notify != nil {
		mq.notify.newMessageNotification()
	}
	if err != nil {
		mq.log.Error("[Q:%s:%d] Moved to file storage with error result [%s] ", mq.name, mq.list[*curidx].idx, err.Error())
		select {
		case mq.list[*curidx].ch <- false:
		default:
		}
		mq.size -= int32(len(mq.list[*curidx].buf))
		copy(mq.list[*curidx:mq.cnt], mq.list[*curidx+1:mq.cnt])
		mq.cnt--
		*curidx--
		return
	}
	if mq.list[*curidx].isBusy {
		mq.list[*curidx].filestorageidx = idx
		mq.log.Trace("[Q:%s:%d] copied to file storage with new idx [%d] ", mq.name, mq.list[*curidx].idx, idx)
	} else {
		select {
		case mq.list[*curidx].ch <- false:
		default:
		}
		mq.log.Trace("[Q:%s:%d] moveid to file storage with new idx [%d] ", mq.name, mq.list[*curidx].idx, idx)
		mq.size -= int32(len(mq.list[*curidx].buf))
		copy(mq.list[*curidx:mq.cnt], mq.list[*curidx+1:mq.cnt])
		mq.cnt--
		*curidx--

	}
}

func (mq *queueMemory) checkTimeOut(CurrentDuration time.Duration) {
	for i := 0; i < int(mq.cnt); i++ {
		if mq.list[i].timeOut < CurrentDuration {
			mq.processTimeOut(&i)
		}
	}
}

func (mq *queueMemory) Close() {
	mq.Lock()
	defer mq.Unlock()
	mq.checkTimeOut(0x7FFFFFFFFFFFFFFF)
}

func (mq *queueMemory) description() string {
	return "memory storage"
}

func (mq *queueMemory) FreeRecord(idx StorageIdx) error {
	var tmp *queueMemoryItem
	mq.Lock()
	defer mq.Unlock()
	CurrentDuration := time.Since(startTime)
	for i := 0; i < int(mq.cnt); i++ {
		if mq.list[i].idx == idx {
			tmp = mq.list[i]
			copy(mq.list[i:mq.cnt], mq.list[i+1:mq.cnt])
			mq.cnt--
			mq.size -= int32(len(tmp.buf))
			i--
			// send notification to waitnig timeout goroutine about processed message
			if tmp.filestorageidx == InvalidIdx {
				select {
				case tmp.ch <- true:
				default:
				}
			} else {
				mq.putter.FreeRecord(tmp.filestorageidx)
			}
		} else {
			if mq.list[i].timeOut < CurrentDuration {
				mq.processTimeOut(&i)
			}
		}
	}
	return nil
}

func (mq *queueMemory) UnlockRecord(idx StorageIdx) error {
	var (
		err error
		Idx StorageIdx
	)
	mq.Lock()
	defer mq.Unlock()
	Idx = InvalidIdx
	CurrentDuration := time.Since(startTime)
	for i := 0; i < int(mq.cnt); i++ {
		if mq.list[i].idx == idx {
			if mq.list[i].filestorageidx != InvalidIdx {
				err = mq.putter.UnlockRecord(mq.list[i].filestorageidx)
			} else {
				Idx, err = mq.putter.put(mq.list[i].buf, putRecordAsProcessedWithError)
			}
			if err == nil && mq.notify != nil {
				mq.notify.newMessageNotification()
			}

			select {
			case mq.list[i].ch <- err == nil:
			default:
			}
			if err != nil {
				return err
			}
			if Idx != InvalidIdx {
				mq.log.Trace("[Q:%s:%d] moved to file storage with new idx [%d] ", mq.name, mq.list[i].idx, Idx)
			}
			// remove from list
			mq.size -= int32(len(mq.list[i].buf))
			copy(mq.list[i:mq.cnt], mq.list[i+1:mq.cnt])
			mq.cnt--
			i--
		} else {
			if mq.list[i].timeOut < CurrentDuration {
				mq.processTimeOut(&i)
			}
		}
	}
	return nil
}

func (mq *queueMemory) Put(buf []byte, Chan chan bool) (StorageIdx, error) {
	mq.Lock()
	defer mq.Unlock()
	if mq.cnt == mq.maxCnt {
		return InvalidIdx, &queueError{ErrorType: errorOverCount}
	}
	if mq.size+int32(len(buf)) >= mq.maxSize {
		return InvalidIdx, &queueError{ErrorType: errorOverSize}
	}
	tmp := &queueMemoryItem{
		buf:            buf,
		ch:             Chan,
		idx:            mq.idx,
		filestorageidx: InvalidIdx,
		timeOut:        time.Since(startTime) + mq.timeout,
	}
	mq.list[mq.cnt] = tmp
	mq.cnt++
	mq.idx++
	mq.size += int32(len(buf))
	return tmp.idx, nil
}

func (mq *queueMemory) Get() (*queueMemoryItem, error) {
	mq.Lock()
	defer mq.Unlock()
	CurrentDuration := time.Since(startTime)
	for i := 0; i < int(mq.cnt); i++ {
		if mq.list[i].timeOut < CurrentDuration {
			mq.processTimeOut(&i)
			continue
		}
		if !mq.list[i].isBusy {
			mq.list[i].isBusy = true
			return mq.list[i], nil
		}
	}
	return nil, &queueError{ErrorType: errorNoMore}
}

func (mq *queueMemory) Count() uint64 {
	mq.Lock()
	defer mq.Unlock()
	return mq.cnt
}

func (mq *queueMemory) Size() uint64 {
	mq.Lock()
	defer mq.Unlock()
	return uint64(mq.size)
}
