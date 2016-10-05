package queue

import (
	"fmt"
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
	fs.Close()
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
	fs.Close()
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
	stat, err := os.Stat(TestFolder + "index.dat")
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
	fs.Close()
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
	i := 0
	for i = 0; i < 500; i++ {
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
		if TestStrings[odd] != string(z.Buffer) {
			t.Fatalf("Invalid value of the storage\n")
		}
		odd++
	}
	if err != nil {
		if _, ok := err.(*queueError); !ok {
			t.Fatalf("Error reading from storage: %s", err)
		}
	}
	fs.Close()

	fs, err = createStorage("Test", TestFolder, nil, &options, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	odd = 0
	for z, _ := fs.Get(); err == nil; z, err = fs.Get() {
		if TestStrings[odd] != string(z.Buffer) {
			t.Fatalf("Invalid value of the storage\n")
		}
		fs.FreeRecord(z.idx)
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
	fs.Close()
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
	testString := string(z.Buffer)
	if TestStrings[1] != testString {
		t.Fatalf("Invalid value in the storage. Returned: %s  Must return second elementh because first is busy yet", testString)
	}
	MySleep(500)
	z, err = fs.Get()
	zz := string(z.Buffer)
	if TestStrings[0] != zz {
		t.Fatalf("Invalid value in the storage : %s.  Must return first element because timeout expired ", zz)
	}
	fs.Close()
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
	fs.Close()
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
		fs.FreeRecord(z.idx)
	}
	z, err := fs.Get()
	if err != nil {
		t.Fatalf("Error reading from storage: %s", err)
	}
	fs.FreeRecord(z.idx)
	z, err = fs.Get()
	if err != nil {
		t.Fatalf("Error reading from storage: %s", err)
	}
	fs.FreeRecord(z.idx)
	fs.Close()
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
	fs.Close()
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
	fs.Close()
}

func TestWorkStorageCheckMoveErroredMessages(t *testing.T) {
	clearTestFolder()
	fs, err := createStorage("Test", TestFolder, nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("Cannot create storage: %s", err)
	}
	fs.Put(make([]byte, 1000))
	fs.Put(make([]byte, 1000))
	fs.Put(make([]byte, 1000))
	a, _ := fs.Get()
	b, _ := fs.Get()
	fs.UnlockRecord(a.idx)
	fs.FreeRecord(b.idx)
	fs.Get()
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
	fs.Close()
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
	fs.Close()
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
	fs.Close()
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
	fs.Close()
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
