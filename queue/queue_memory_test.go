package queue

import (
	"errors"
	"testing"
	"time"
)

// empty storage for checking of the memory queue
type testPutter struct {
	ID  StorageIdx
	Cnt int
}

func (tp *testPutter) put(buffer []byte, option int) (StorageIdx, error) {
	tp.ID++
	return tp.ID - 1, nil
}
func (tp *testPutter) UnlockRecord(Idx StorageIdx) error {
	tp.Cnt++
	return nil
}

func (tp *testPutter) FreeRecord(Idx StorageIdx) error {
	if Idx == 0 {
		tp.Cnt = 100
	}
	return nil
}

type testPutterError struct {
}

func (tp *testPutterError) put(buffer []byte, option int) (StorageIdx, error) {
	return InvalidIdx, errors.New("Some error")
}
func (tp *testPutterError) UnlockRecord(Idx StorageIdx) error {
	return errors.New("Some error")
}

func (tp *testPutterError) FreeRecord(Idx StorageIdx) error {
	return nil
}

func TestMemoryQueueClose(t *testing.T) {
	MessagesCount := 10
	putter := &testPutter{}
	mq := createMemoryQueue("", 1024, 1024*1024*16, putter, nil, time.Millisecond*500, nil)
	if mq == nil {
		t.Fatal("Cannot create memory storage")
	}
	for i := 0; i < MessagesCount; i++ {
		mq.Put(make([]byte, 100), nil)
	}
	mq.Close()
	if putter.ID != StorageIdx(MessagesCount) {
		t.Fatalf("Putter did not receive all messages. Received %d/%d.", putter.ID, MessagesCount)
	}
	if mq.Count() != 0 {
		t.Fatalf("After close of the storage in present  %d messages.", mq.Count())
	}
	if mq.Size() != 0 {
		t.Fatalf("After close of the storage size is %d bytes.", mq.Size())
	}
}

func TestMemoryQueueOverLengthAndOverSize(t *testing.T) {
	MessagesCount := 10
	putter := &testPutter{}
	mq := createMemoryQueue("", uint16(MessagesCount), 1024*1024, putter, nil, time.Millisecond*500, nil)
	if mq == nil {
		t.Fatal("Cannot create memory storage")
	}
	for i := 0; i < MessagesCount; i++ {
		mq.Put(make([]byte, 100), nil)
	}
	if _, err := mq.Put(make([]byte, 100), nil); err == nil {
		t.Fatal("Not passed overlength test")
	}
	mq.Close()
	for i := 0; i < 2; i++ {
		mq.Put(make([]byte, 510*1024), nil)
	}
	if _, err := mq.Put(make([]byte, 4097), nil); err == nil {
		t.Fatal("Not passed oversize test")
	}
}

func TestMemoryQueueTimeouts(t *testing.T) {
	//	MessagesCount := 10
	putter := &testPutter{}
	mq := createMemoryQueue("", 1024, 1024*1024*16, putter, nil, time.Millisecond*100, nil)
	if mq == nil {
		t.Fatal("Cannot create memory storage")
	}
}

func TestMemoryQueueTestInProcessSkip(t *testing.T) {
	//	MessagesCount := 10
	putter := &testPutter{}
	mq := createMemoryQueue("", 1024, 1024*1024*16, putter, nil, time.Millisecond*100, nil)
	if mq == nil {
		t.Fatal("Cannot create memory storage")
	}
	mq.Put(make([]byte, 100), nil)
	data, _ := mq.Get()
	mq.UnlockRecord(data.idx)
	if putter.ID != 1 {
		t.Fatal("Cannot move error message to file storage")
	}
}

func TestMemoryQueueTestInTimeOut(t *testing.T) {
	//	MessagesCount := 10
	putter := &testPutter{}
	mq := createMemoryQueue("", 1024, 1024*1024*16, putter, nil, time.Millisecond*50, nil)
	if mq == nil {
		t.Fatal("Cannot create memory storage")
	}
	mq.Put(make([]byte, 100), nil)
	mq.Put(make([]byte, 100), nil)
	time.Sleep(time.Millisecond * 50)
	_, err := mq.Get()
	if err == nil {
		t.Fatal("records must be moved to file storage")
	}
}

func TestMemoryQueueTestInTimeOutWithError(t *testing.T) {
	putter := &testPutterError{}
	mq := createMemoryQueue("", 1024, 1024*1024*16, putter, nil, time.Millisecond*50, nil)
	if mq == nil {
		t.Fatal("Cannot create memory storage")
	}
	ch := make(chan bool)
	timer := time.After(500 * time.Millisecond)
	go func() {
		mq.Put(make([]byte, 100), ch)
		time.Sleep(time.Millisecond * 50)
		mq.Get()
	}()
	processed := false
	state := false
	select {
	case state = <-ch:
		processed = true
	case <-timer:
	}
	if !processed || state {
		t.Fatal("function must return false")
	}
}

func TestMemoryQueueTestInTimeOutProcessedRecord(t *testing.T) {
	putter := &testPutter{}
	mq := createMemoryQueue("", 1024, 1024*1024*16, putter, nil, time.Millisecond*50, nil)
	if mq == nil {
		t.Fatal("Cannot create memory storage")
	}
	mq.Put(make([]byte, 100), nil)
	data, _ := mq.Get()
	time.Sleep(time.Millisecond * 50)
	mq.Get()
	mq.FreeRecord(data.idx)
	if putter.Cnt != 100 {
		t.Fatal("Processed record must be removed from file storage too.")
	}
}
