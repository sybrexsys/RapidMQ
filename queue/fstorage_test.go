package queue

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

func clearTestFolder() {
	os.RemoveAll(TestFolder)
	os.MkdirAll(TestFolder, 040777)
	os.MkdirAll(logFolder, 040777)
}

var TestStrings = [10]string{
	"one",
	"Two",
	"Three",
	"Four",
	"Five",
	"Six",
	"Seven",
	"Eight",
	"None",
	"Ten",
}

func readString(r io.Reader) string {
	if r == nil {
		return ""
	}
	buf := bufio.NewScanner(r)
	buf.Scan()
	return buf.Text()
}

func MySleep(ms time.Duration) {
	m := time.After(ms * time.Millisecond)
	<-m
}

func TestCreateNewStorage(t *testing.T) {
	clearTestFolder()
	fs, err := createStorage("Test", TestFolder, nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
}

func TestDecreaseSizeOfIndexFile(t *testing.T) {

	clearTestFolder()
	fs, err := createStorage("Test", TestFolder, nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	var buffer [50000]byte
	for i := 0; i < 2000; i++ {
		fs.Put(buffer[:])
	}
	i := 0
	var z *QueueItem
	for i = 0; i < 2000; i++ {
		z, err = fs.Get()
		if err != nil {
			t.Fatalf("Error reading from storage: %s", err)
		}
		fs.FreeRecord(z.idx)
	}
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
	stat, err1 := os.Stat(TestFolder + "index.dat")
	if err1 != nil {
		t.Fatalf("Index file was not found")
	}
	if stat.Size() > 1<<15 {
		t.Fatalf("Index file is very large")
	}

}

func TestFillStorage(t *testing.T) {
	var buffer [50000]byte
	clearTestFolder()
	options := DefaultStorageOptions
	options.MaxDataFileSize = 301000
	fs, err := createStorage("Test", TestFolder, nil, &options, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	for i := 0; i < 7; i++ {
		fs.Put(buffer[:])
	}
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
	_, err = os.Stat(TestFolder + "index.dat")
	if err != nil {
		t.Fatalf("Not found index file: %s", err)
	}
	_, err = os.Stat(TestFolder + dataFileNameByID(0))
	if err != nil {
		t.Fatalf("Not found first data file: %s", err)
	}
	_, err = os.Stat(TestFolder + dataFileNameByID(6))
	if err != nil {
		t.Fatalf("Not found second data file: %s", err)
	}
}

func TestFillIncrementIndex(t *testing.T) {
	var buffer [100]byte
	clearTestFolder()
	options := DefaultStorageOptions
	options.MaxDataFileSize = 301000
	fs, err := createStorage("Test", TestFolder, nil, &options, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	stat, _ := os.Stat(TestFolder + "index.dat")
	reccount := int64(0x8000)
	if stat.Size() != reccount {
		t.Fatalf("Invalid size of the index file")
	}
	for i := 0; i < 1000; i++ {
		fs.Put(buffer[:])
	}
	if stat.Size() != 0x8000 {
		t.Fatalf("Invalid size of the index file")
	}
	for i := 0; i < 1000; i++ {
		fs.Put(buffer[:])
	}
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
	stat, err = os.Stat(TestFolder + "index.dat")
	if err != nil {
		t.Fatalf("Not found index file: %s", err)
	}
	if stat.Size() < 0xffff {
		t.Fatalf("Not incremented index file")
	}

}

func TestFillingData(t *testing.T) {

	var z *QueueItem

	clearTestFolder()
	options := DefaultStorageOptions
	fs, err := createStorage("Test", TestFolder, nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	for i := 0; i < 500; i++ {
		fs.Put([]byte("Test of the work with all structures"))
	}
	for i := 0; i < 500; i++ {
		z, err = fs.Get()
		if err != nil {
			t.Fatalf("Error reading from storage: %s", err)
		}
		fs.FreeRecord(z.idx)
	}
	for i := 0; i < 1000; i++ {
		fs.Put([]byte("Test of the work with all structures"))
	}
	for _, k := range TestStrings {
		fs.Put([]byte(k))
	}
	for i := 0; i < 1000; i++ {
		z, err = fs.Get()
		if err != nil {
			t.Fatalf("Error reading from storage: %s", err)
		}
		fs.FreeRecord(z.idx)
	}
	if fs.Count() != 10 {
		t.Fatalf("Not release full storage: ")
	}
	odd := 0
	for z, err = fs.Get(); err == nil; z, err = fs.Get() {
		if TestStrings[odd] != readString(z.Stream) {
			t.Fatalf("Invalid value of the storage\n")
		}
		odd++
	}
	if err != nil {
		if _, ok := err.(*queueError); !ok {
			t.Fatalf("Error reading from storage: %s", err)
		}
	}
	fs.info()
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
	fs, err = createStorage("Test", TestFolder, nil, &options, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	odd = 0
	for z, _ := fs.Get(); err == nil; z, err = fs.Get() {
		if TestStrings[odd] != readString(z.Stream) {
			t.Fatalf("Invalid value of the storage\n")
		}
		err = fs.FreeRecord(z.idx)
		if err != nil {
			t.Fatalf("Error free record: %s", err)
		}
		odd++
	}
	if err != nil {
		if _, ok := err.(*queueError); !ok {
			t.Fatalf("Error reading from storage: %s", err)
		}
	}
	for i := 0; i < 500; i++ {
		fs.Put([]byte("Test of the work with all structures"))
	}
	for i := 0; i < 500; i++ {
		z, err = fs.Get()
		if err != nil {
			t.Fatalf("Error reading from storage: %s", err)
		}
		fs.FreeRecord(z.idx)
	}
	if fs.Count() != 0 {
		t.Fatalf("Not release full storage: ")

	}
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
}

func TestWorkWithReleaseRecord(t *testing.T) {

	var z *QueueItem
	clearTestFolder()
	fs, err := createStorage("Test", TestFolder, nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	for _, k := range TestStrings {
		fs.Put([]byte(k))
	}
	z, err = fs.Get()
	fs.UnlockRecord(z.idx)
	z, err = fs.Get()
	testString := readString(z.Stream)
	if TestStrings[1] != testString {
		t.Fatalf("Invalid value in the storage. Returned: %s  Must return second elementh because first is busy yet", testString)
	}
	MySleep(500)
	z, err = fs.Get()
	zz := readString(z.Stream)
	if TestStrings[0] != zz {
		t.Fatalf("Invalid value in the storage : %s.  Must return first element because timeout expired ", zz)
	}
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
}

func TestWorkStorageSize(t *testing.T) {
	clearTestFolder()
	fs, err := createStorage("Test", TestFolder, nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	for _, k := range TestStrings {
		fs.Put([]byte(k))
	}
	if fs.Count() != 10 {
		t.Fatalf("Not release full storage: ")
	}
	r := 0
	for _, k := range TestStrings {
		r += len([]byte(k))
	}
	if uint64(r) != fs.RecordsSize() {
		t.Fatalf("Invalid value of the storage\n")
	}
	fs.info()
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
}

func TestFillDeleteUnusedDataFiles(t *testing.T) {
	var buffer [2000]byte
	clearTestFolder()
	zzz := 99
	options := DefaultStorageOptions
	options.MaxDataFileSize = 200000
	options.CheckCRCOnRead = true
	fs, err := createStorage("Test", TestFolder, nil, &options, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	for i := 0; i < zzz; i++ {
		fs.Put(buffer[:])
	}
	for i := 0; i < zzz-2; i++ {
		z, err := fs.Get()
		if err != nil {
			t.Fatalf("Error reading from storage: %s", err)
		}
		err = fs.FreeRecord(z.idx)
		if err != nil {
			t.Fatalf("Error free record: %s", err)
		}
	}
	z, err := fs.Get()
	if err != nil {
		t.Fatalf("Error reading from storage: %s", err)
	}
	err = fs.FreeRecord(z.idx)
	if err != nil {
		t.Fatalf("Error free record: %s", err)
	}
	z, err = fs.Get()
	if err != nil {
		t.Fatalf("Error reading from storage: %s", err)
	}
	fs.FreeRecord(z.idx)
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
	_, err = os.Stat(TestFolder + "stg00000.dat")
	if err == nil {
		t.Fatalf("File with unused data was not delete")
	}
}

func TestErrors(t *testing.T) {

	var z *QueueItem
	clearTestFolder()
	fs, err := createStorage("Test", TestFolder, nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	for zq, k := range TestStrings {
		if zq > 0 {
			break
		}
		fs.Put([]byte(k))
	}
	z, err = fs.Get()
	if err != nil {
		t.Fatalf("Cannot receive item. Err: %v", err)
	}
	fs.UnlockRecord(z.idx)
	z, err = fs.Get()
	if err != nil {
		if _, ok := err.(*queueError); !ok {
			t.Fatalf("Must create filestorage error, but receive %v", err)
		}
	} else {
		t.Fatalf("Must create filestorage error")
	}
	MySleep(500)
	fs.Get()
	_, err = fs.Get()
	if err != nil {
		if _, ok := err.(*queueError); !ok {
			t.Fatalf("Must create filestorage error, but receive %v", err)
		}
	} else {
		t.Fatalf("Must create filestorage error")
	}
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
}

func TestParralels(t *testing.T) {

	clearTestFolder()
	var tmp [0x3fff]byte
	fs, err := createStorage("Test", TestFolder, nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	var wg sync.WaitGroup
	for i := int64(0); i < 20; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				fs.Put(tmp[:])
				fs.Put([]byte(fmt.Sprintf("Routine:%d Iteration %d", id, i)))
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for z, err := fs.Get(); err == nil; z, err = fs.Get() {
				fs.FreeRecord(z.idx)
			}
		}(i)
	}
	wg.Wait()
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
}

func TestWorkStorageCheckMoveErroredMessages(t *testing.T) {
	clearTestFolder()
	fs, err := createStorage("Test", TestFolder, nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}

	_, err = fs.Put(make([]byte, 1000))
	if err != nil {
		t.Fatalf("Cannot put to storage: %s", err)
	}
	_, err = fs.Put(make([]byte, 1000))
	if err != nil {
		t.Fatalf("Cannot put to storage: %s", err)
	}
	_, err = fs.Put(make([]byte, 1000))
	if err != nil {
		t.Fatalf("Cannot put to storage: %s", err)
	}
	a, err := fs.Get()
	if err != nil {
		t.Fatalf("Cannot get from storage: %s", err)
	}
	b, err := fs.Get()
	if err != nil {
		t.Fatalf("Cannot get from storage: %s", err)
	}
	err = fs.UnlockRecord(a.idx)
	if err != nil {
		t.Fatalf("Cannot unlock record: %s", err)
	}
	err = fs.FreeRecord(b.idx)
	if err != nil {
		t.Fatalf("Cannot free record: %s", err)
	}
	_, err = fs.Get()
	if err != nil {
		t.Fatal("Must not be error. C element must be returned")
	}
	_, err = fs.Get()
	if err == nil {
		t.Fatal("Must be error. 'Will available when' must be")
	}
	e := err.Error()
	myerr, ok := err.(*queueError)
	if !ok || myerr.ErrorType != errorInDelay {
		t.Fatalf("Must be internal error. 'Will available when' must be. Found:%s", e)
	}
	time.Sleep(myerr.NextAvailable)
	a, err = fs.Get()
	if err != nil {
		t.Fatal("Must be return valid value")
	}
	if a.idx != 1 {
		t.Fatalf("Must be return index value 1, returned: %d", a.idx)
	}
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
}

func TestWorkStorageRestore(t *testing.T) {
	clearTestFolder()
	fs, err := createStorage("Test", TestFolder, nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	for _, k := range TestStrings {
		fs.Put([]byte(k))
	}
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
	os.Remove(TestFolder + "index.dat")
	Handle, _ := os.Create(TestFolder + dataFileNameByID(100))
	Handle.Close()
	Handle, _ = os.Create(TestFolder + "test" + dataFileNameByID(100))
	Handle.Close()
	fs, err = createStorage("Test", TestFolder, nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	if fs.Count() != uint64(len(TestStrings)) {
		t.Fatalf("Put %d messages. Restored: %d", len(TestStrings), fs.Count())
	}
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
	Handle, _ = os.Create(TestFolder + dataFileNameByID(100))
	Handle.Close()
	Handle, _ = os.Create(TestFolder + "test" + dataFileNameByID(100))
	Handle.Close()

	fs, err = createStorage("Test", TestFolder, nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	if fs.Count() != uint64(len(TestStrings)) {
		t.Fatalf("Put %d messages. Restored: %d", len(TestStrings), fs.Count())
	}
	err = fs.Close()
	if err != nil {
		t.Fatalf("Cannot close storage: %s", err)
	}
}

var TestFolder string
var logFolder string

func init() {
	tmp := normalizeFilePath(os.TempDir())
	if runtime.GOOS == "windows" {
		TestFolder = tmp + "queue\\"
		logFolder = tmp + "queue\\log\\"
	} else {
		TestFolder = tmp + "queue/"
		logFolder = tmp + "queue/log/"
	}

}
