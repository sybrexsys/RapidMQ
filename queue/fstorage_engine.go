package queue

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/sybrexsys/RapidMQ/queue/internal/mmap"
)

var startTime = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// InvalidIdx is Invalid index description
const InvalidIdx StorageIdx = 0xFFFFFFFFFFFFFFFF

var magicNumberDataPrefix = uint32(0x67452301)
var magicNumberDataSuffix = uint32(0xEFCDAB89)

type fileAccess struct {
	*sync.Mutex
	Handle *os.File
}

//StorageIdx is unique identifier of the message in the memory or on the disk
type StorageIdx uint64

const (
	stateEnable = iota
	stateInProcess
	stateFree
)

const (
	putRecordAsNew = iota
	putRecordAsInProcess
	putRecordAsProcessedWithError
)

type storagePutter interface {
	put(buffer []byte, option int) (StorageIdx, error)
	UnlockRecord(Idx StorageIdx) error
	FreeRecord(Idx StorageIdx) error
}

var dot = [1]byte{0}

// availableRecordInfo is Structure returned by GetNext function for receive information about available record
type availableRecordInfo struct {
	Idx      StorageIdx
	ID       StorageIdx
	FileInfo fileAccess

	FileOffset uint32
	Length     int32
}

type indexFileHeader struct {
	MagicNumber     uint64
	TotalRecord     uint64
	TotalFree       uint64
	MinIndex        StorageIdx
	CRC             int32
	IndexRecordSize int32
}

type indexRecord struct {
	ID         StorageIdx
	FileIndex  StorageIdx
	LastAction time.Duration
	State      int32
	FileOffset uint32
	Length     int32
	TryCount   int32
}

var (
	lastRecord = indexRecord{
		FileIndex:  InvalidIdx,
		FileOffset: 0xFFFFFFFF,
		Length:     -1,
		State:      -1,
		TryCount:   -1,
	}

	emptyRecord = indexRecord{
		FileIndex:  InvalidIdx,
		FileOffset: 0xFFFFFFFF,
		Length:     -1,
		State:      -1,
		TryCount:   -1,
	}
)

// fileStorage is struct for present disk storage of the data
type fileStorage struct {
	folder                    string                    // folder where locatd files of the storage
	log                       Logging                   // log file for store information about actions
	name                      string                    //name of then storage. At current time used for save to log file only
	options                   *StorageOptions           // storage options
	readMutex                 sync.RWMutex              // mutex for work with file handles for output from the sorage
	readFiles                 map[StorageIdx]fileAccess // map for store read handles of the storage
	writeFiles                *filequeue                // list of the opened handles for writing
	idxFile                   *os.File
	mmapinfo                  mmap.MMap
	mmapsize                  int64
	idxMutex                  sync.Mutex
	idx                       []indexRecord
	operations                uint32
	freeCounts                map[StorageIdx]int64
	startIdx                  uint64
	lastTimeCheckDeletedFiles time.Duration
	timeout                   time.Duration
	notify                    newMessageNotificator
	*indexFileHeader
}

// newID appends new item into storage, test size of the mapped file and increase this value if not enough
func (fs *fileStorage) newID() (StorageIdx, error) {
	if fs.TotalRecord == uint64(len(fs.idx)-1) {
		// Check possibility to move index for processed elements of the index
		if !fs.checkFreeIndexRecords(true) {
			fs.mmapinfo.Unmap()
			indexFileSize, err := fs.calculateNextSize(fs.mmapsize + 1)
			if err != nil {
				fs.idxFile.Close()
				return InvalidIdx, err
			}
			fs.idxFile.Seek(indexFileSize-1, 0)
			fs.idxFile.Write(dot[0:1])
			fs.mmapinfo, err = mmap.MapRegion(fs.idxFile, int(indexFileSize), mmap.RDWR, 0, 0)
			if err != nil {
				fs.idxFile.Close()
				return InvalidIdx, err
			}
			fs.mmapsize = indexFileSize
			fs.setMMapInfo()
		}
	}
	idx := StorageIdx(fs.TotalRecord) + fs.MinIndex
	fs.idx[fs.TotalRecord] = emptyRecord
	fs.TotalRecord++
	fs.idx[fs.TotalRecord] = lastRecord
	return idx, nil
}

// RecordsSize returns size of the all available records
func (fs *fileStorage) RecordsSize() uint64 {
	var total uint64
	fs.idxMutex.Lock()
	defer fs.idxMutex.Unlock()
	for i := fs.startIdx; i < fs.TotalRecord; i++ {
		if fs.idx[i].State != stateFree {
			total += uint64(fs.idx[i].Length)
		}
	}
	return total
}

// Count returns count of the all available records
func (fs *fileStorage) Count() uint64 {
	fs.idxMutex.Lock()
	defer fs.idxMutex.Unlock()
	return fs.TotalRecord - fs.TotalFree
}

// description is used in logging system for detect source of the processing message
func (fs *fileStorage) description() string {
	return "disk storage"
}

//GetNext returns information about next available record and mark this record as InProgress
//Possible unmark this record as Enabled with  UnlockRecord function or
// mark record as free with FreeRecord function
func (fs *fileStorage) getNext() (*availableRecordInfo, error) {
	var err error
	firstSkiped := InvalidIdx
	lastFree := InvalidIdx
	NextDuration := time.Duration(0x7FFFFFFFFFFFFFFF)
	fs.idxMutex.Lock()
	defer fs.idxMutex.Unlock()
	checkStartIdx := true
	for i := fs.startIdx; i < fs.TotalRecord; i++ {
		if checkStartIdx && fs.idx[i].State != stateFree {
			fs.startIdx = i
			checkStartIdx = false
		}
		if fs.idx[i].State == stateFree && firstSkiped != InvalidIdx {
			lastFree = StorageIdx(i)
		}
		if fs.idx[i].State == stateEnable {
			if fs.options.SkipReturnedRecords && fs.idx[i].TryCount > 0 {
				//Check record to overdue timeout to current time
				CurrentTime := time.Since(startTime)
				TmpDuration := fs.idx[i].LastAction + time.Duration(fs.options.SkipDelayPerTry)*time.Duration(fs.idx[i].TryCount)*time.Millisecond
				if TmpDuration >= CurrentTime {
					if TmpDuration < NextDuration {
						NextDuration = TmpDuration
					}
					if firstSkiped == InvalidIdx {
						firstSkiped = StorageIdx(i)
					}
					continue
				}
			}
			if lastFree != InvalidIdx {
				fs.idx[lastFree] = fs.idx[firstSkiped]
				fs.idx[firstSkiped].State = stateFree
			}
			tmp := &availableRecordInfo{
				Idx:        fs.MinIndex + StorageIdx(i),
				FileOffset: fs.idx[i].FileOffset,
				Length:     fs.idx[i].Length,
				ID:         fs.idx[i].ID,
			}
			var (
				file fileAccess
				ok   bool
			)
			FileIdx := fs.idx[i].FileIndex
			// get open file handle for read record
			fs.readMutex.RLock()
			file, ok = fs.readFiles[FileIdx]
			fs.readMutex.RUnlock()
			if !ok {
				file.Handle, err = os.Open(fs.folder + dataFileNameByID(FileIdx))
				if err != nil {
					fs.log.Error("[QFS:%s:%d] Cannot open datafile {%d}  Error:[%s]", fs.name, tmp.ID, FileIdx, err.Error())
					return nil, err
				}
				fs.log.Trace("[QFS:%s:%d] Was opened datafile {%d}", fs.name, tmp.ID, FileIdx)
				file.Mutex = &sync.Mutex{}
				fs.readMutex.Lock()

				// Check count of the opened files and if exceed remove one open handle from the map
				if int16(len(fs.readFiles)) > fs.options.MaxOneTimeOpenedFiles {
					for k, vak := range fs.readFiles {
						vak.Lock()
						vak.Handle.Close()
						vak.Unlock()
						delete(fs.readFiles, k)
						break
					}
				}
				fs.readFiles[FileIdx] = file
				fs.readMutex.Unlock()
			}
			fs.idx[i].State = stateInProcess
			tmp.FileInfo = file
			fs.checkFlush()
			return tmp, nil
		}
	}
	// Check to messages with timeout and return nearest time of the ability of the recordd
	if NextDuration != 0x7FFFFFFFFFFFFFFF {
		return nil, &queueError{
			ErrorType:     errorInDelay,
			NextAvailable: NextDuration - time.Since(startTime),
		}
	}
	// return error {No more} and timestamp of the checking
	return nil, &queueError{
		ErrorType:     errorNoMore,
		NextAvailable: time.Since(startTime),
	}
}

// Get returns next available record from the storage
func (fs *fileStorage) Get() (*Message, error) {
	var buf []byte

	for {
		ai, err := fs.getNext()
		if err != nil {
			return nil, err
		}
		// if is not valid message we remove it from queue
		if buf = fs.getValidRecord(ai.FileInfo, ai.ID, ai.FileOffset, ai.Length); buf == nil {
			fs.freeRecord(ai.Idx)
			continue
		}
		tmp := &Message{
			Buffer:  buf,
			ID:      ai.ID,
			idx:     ai.Idx,
			storage: fs,
		}
		return tmp, nil
	}
}

// UnlockRecord unmarks record with index as enabled for next select
func (fs *fileStorage) UnlockRecord(Idx StorageIdx) error {
	fs.idxMutex.Lock()
	defer fs.idxMutex.Unlock()
	if Idx < fs.MinIndex || Idx-fs.MinIndex >= StorageIdx(fs.TotalRecord) {
		return errors.New("out of bound")
	}
	i := uint64(Idx - fs.MinIndex)
	if fs.idx[i].State == stateInProcess {
		fs.idx[i].State = stateEnable
		fs.idx[i].TryCount++
		fs.idx[i].LastAction = time.Since(startTime)
		fs.checkFlush()
	}
	if fs.notify != nil {
		fs.notify.newMessageNotification()
	}
	return nil
}

// UnlockRecord unmarks record with index as enabled for next select
func (fs *fileStorage) checkUnusedFiles() {
	for FileIdx, count := range fs.freeCounts {
		if count == 0 && fs.writeFiles.canClear(FileIdx) {
			fs.log.Trace("[QFS:%s] Delete datafile {%d} because he does not contain any data", fs.name, FileIdx)
			fs.readMutex.Lock()
			file, ok := fs.readFiles[FileIdx]
			if ok {
				file.Handle.Close()
				file.Handle = nil
				delete(fs.readFiles, FileIdx)
			}
			fs.readMutex.Unlock()
			os.Remove(fs.folder + dataFileNameByID(FileIdx))
			delete(fs.freeCounts, FileIdx)
		}
	}
}

// freeRecord marks record as processed and removes unused file with bodies of the messages
func (fs *fileStorage) freeRecord(Idx StorageIdx) error {
	if Idx < fs.MinIndex || Idx-fs.MinIndex >= StorageIdx(fs.TotalRecord) {
		return errors.New("out of bound")
	}
	localidx := Idx - fs.MinIndex
	if fs.idx[localidx].State != stateFree {
		fs.idx[localidx].State = stateFree
		FileIdx := fs.idx[localidx].FileIndex
		fs.log.Trace("[QFS:%s] Deleted from datafile {%d} count %d ", fs.name, FileIdx, fs.freeCounts[FileIdx])
		fs.freeCounts[FileIdx]--
		if fs.freeCounts[FileIdx] == 0 && fs.writeFiles.canClear(FileIdx) {
			fs.log.Trace("[QFS:%s] Delete datafile {%d} because he does not contain any data", fs.name, FileIdx)
			fs.readMutex.Lock()
			file, ok := fs.readFiles[FileIdx]
			if ok {
				file.Handle.Close()
				file.Handle = nil
				delete(fs.readFiles, FileIdx)
			}
			fs.readMutex.Unlock()
			os.Remove(fs.folder + dataFileNameByID(FileIdx))
			delete(fs.freeCounts, FileIdx)
		} else {
			Now := time.Since(startTime)
			if fs.lastTimeCheckDeletedFiles+30*time.Second < Now {
				fs.checkUnusedFiles()
				fs.lastTimeCheckDeletedFiles = Now
			}
		}

		fs.TotalFree++
	}
	return nil
}

// FreeRecord marks record as free and in the future this record will never returns via GetNext function
func (fs *fileStorage) FreeRecord(Idx StorageIdx) error {
	fs.idxMutex.Lock()
	defer fs.idxMutex.Unlock()
	return fs.freeRecord(Idx)
}

// setMMapInfo sets index slice ond header of the index file to memopry mapped file
func (fs *fileStorage) setMMapInfo() {
	fs.indexFileHeader = (*indexFileHeader)(unsafe.Pointer(&fs.mmapinfo[0])) //#nosec
	head := (*reflect.SliceHeader)(unsafe.Pointer(&fs.idx))                  //#nosec
	sof := unsafe.Sizeof(*fs.indexFileHeader)                                //#nosec
	head.Data = uintptr(unsafe.Pointer(&fs.mmapinfo[sof]))                   //#nosec
	var ir indexRecord
	head.Len = int((uintptr(fs.mmapsize) - sof) / unsafe.Sizeof(ir)) //#nosec
	head.Cap = head.Len
}

// calculateNextSize calculates new size of index file  with increase by twice until 1 Gb size
// After this event increments by 1 Gb
func (fs *fileStorage) calculateNextSize(CurrentSize int64) (int64, error) {
	if CurrentSize == 0 {
		CurrentSize = int64(os.Getpagesize())
	}
	for i := uint(15); i <= 30; i++ {
		if CurrentSize <= 1<<i {
			return 1 << i, nil
		}
	}
	// TODO: Raise error wher index file will be more then 2 Gb for Windows OS ( 32 bits)
	//	if size > maxMapSize {
	//		return 0, fmt.Errorf("mmap too large")
	//	}
	return CurrentSize + 1<<30, nil
}

func (fs *fileStorage) prepareIndexFile() error {
	var err error
	indexFileName := fs.folder + "index.dat"
	fs.idxFile, err = os.OpenFile(indexFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	indexFileSize, err := fs.idxFile.Seek(0, 2)
	if err != nil {
		return err
	}
	isNewFile := (indexFileSize == 0)
	if isNewFile {
		indexFileSize, err = fs.calculateNextSize(0)
		if err != nil {
			return err
		}
		fs.idxFile.Seek(indexFileSize-1, 0)
		fs.idxFile.Write(dot[0:1])
	} else {
		indexFileSize, err = fs.calculateNextSize(indexFileSize)
		if err != nil {
			return err
		}
	}
	fs.mmapinfo, err = mmap.MapRegion(fs.idxFile, int(indexFileSize), mmap.RDWR, 0, 0)
	if err != nil {
		fs.idxFile.Close()
		return err
	}
	fs.mmapsize = indexFileSize
	fs.setMMapInfo()
	if isNewFile {
		var r indexRecord
		fs.MagicNumber = magicNumberValue
		fs.CRC = 0
		fs.IndexRecordSize = int32(unsafe.Sizeof(r)) //#nosec
		fs.MinIndex = 0
		fs.TotalRecord = 0
		fs.TotalFree = 0
	} else {
		if fs.MagicNumber != magicNumberValue {
			return errors.New("invalid index file")
		}
	}
	TotalFree := uint64(0)
	StartCheck := true
	for i := uint64(0); i < fs.TotalRecord; i++ {
		if fs.idx[i].State == stateFree {
			TotalFree++
			continue
		}
		if StartCheck {
			fs.startIdx = i
			StartCheck = false
		}
		//if by some reasons server was incorrect stopped some messages can be marked as in process
		// we used to process them again
		if fs.idx[i].State == stateInProcess {
			fs.idx[i].State = stateEnable
		}
		fs.freeCounts[fs.idx[i].FileIndex]++
	}
	fs.TotalFree = TotalFree
	return nil
}

// loadIndexFile loads index file
// All InProcess record will marked as Available after call this function
func (fs *fileStorage) loadIndexFile() error {
	indexFileName := fs.folder + "index.dat"
	_, err := os.Stat(indexFileName)
	if err != nil {
		path := fs.folder[:len(fs.folder)-1]
		listFiles, err := ioutil.ReadDir(path)
		if err == nil {
			for _, finfo := range listFiles {
				if finfo.IsDir() {
					continue
				}
				fname := finfo.Name()
				if checkValidFileDataName(fname) != -1 {
					return errors.New("index was not found")
				}
				os.Remove(fs.folder + fname)
			}
		}
	}
	fs.prepareIndexFile()
	fs.deleteUnusedFiles()
	return nil
}

//flush does sync of the memory mapped file with disk file
func (fs *fileStorage) flush() {
	if fs.mmapinfo != nil {
		fs.mmapinfo.Flush()
	}
}

// checkFlush checks count of the executed opperations and sync with disk file if need
func (fs *fileStorage) checkFlush() {
	fs.operations++
	if fs.operations >= fs.options.FlushOperations {
		fs.flush()
		fs.operations = 0
	}
}

// checkValidRecord checks validity of the record
func (fs *fileStorage) getValidRecord(file fileAccess, index StorageIdx, offset uint32, length int32) []byte {
	var buff [16]byte
	file.Lock()
	defer file.Unlock()

	file.Handle.Seek(int64(offset), 0)
	io.ReadFull(file.Handle, buff[:])
	z := binary.LittleEndian.Uint32(buff[:])
	if z != magicNumberDataPrefix {
		return nil
	}
	z64 := binary.LittleEndian.Uint64(buff[4:])
	if StorageIdx(z64) != index {
		return nil
	}
	z = binary.LittleEndian.Uint32(buff[12:])
	if z != uint32(length) {
		return nil
	}
	outbuf := make([]byte, z)
	io.ReadFull(file.Handle, outbuf)
	io.ReadFull(file.Handle, buff[:8])
	if fs.options.CheckCRCOnRead {
		crc := crc32.ChecksumIEEE(outbuf)
		z = binary.LittleEndian.Uint32(buff[:])
		if crc != z {
			return nil
		}
	}
	z = binary.LittleEndian.Uint32(buff[4:])
	if z != magicNumberDataSuffix {
		return nil
	}
	return outbuf
}

// checkFreeIndexRecords checks count of the free records in the top of the index table and removes such records
func (fs *fileStorage) checkFreeIndexRecords(IsIncrementIndex bool) bool {
	var Frees uint64
	for i := uint64(0); i < fs.TotalRecord; i++ {
		if fs.idx[i].State == stateFree {
			Frees++
		} else {
			break
		}
	}
	percents := uint8(100 * Frees / uint64(len(fs.idx)))
	if (IsIncrementIndex && percents < fs.options.PercentFreeForRecalculateOnIncrementIndexFile) ||
		percents < fs.options.PercentFreeForRecalculateOnExit {
		return false
	}
	fs.flush()
	copy(fs.idx[0:], fs.idx[Frees:fs.TotalRecord])
	fs.TotalRecord -= Frees
	fs.TotalFree -= Frees
	fs.log.Trace("[QFS:%s] Total free count %d", fs.name, fs.TotalFree)
	fs.MinIndex += StorageIdx(Frees)
	fs.startIdx = 0
	/*
		for i := uint64(1); i <= Frees && newidx+i < uint64(len(fs.idx)); i++ {
			fs.idx[newidx+i] = indexRecord{}
		}
		fs.idx[newidx] = lastRecord*/
	fs.flush()
	return true
}

//put appends one message to storage and marks state in depend of the option

func (fs *fileStorage) put(buffer []byte, option int) (StorageIdx, error) {
	var (
		tmp    indexRecord
		offset int64
	)

	fs.idxMutex.Lock()
	Idx, err := fs.newID()
	if err != nil {
		return InvalidIdx, err
	}
	fs.idxMutex.Unlock()
	fs.log.Trace("[QFS:%s:%d] Resuest write handle", fs.name, Idx)
	file, err := fs.writeFiles.getHandle(uint32(len(buffer)), Idx, fs.timeout)
	if err != nil {
		fs.log.Error("[QFS:%s%d] Cannot receive write handle Error: %s", fs.name, Idx, err.Error())
		return InvalidIdx, err
	}
	defer fs.writeFiles.putHandle(file, err)
	offset, err = file.Seek(0, 2)
	if err != nil {
		fs.log.Error("[QFS:%s:%d] Cannot append to datafile {%d} Error: %s", fs.name, Idx, file.FileIdx, err.Error())
		return InvalidIdx, err
	}
	err = saveDataFileData(file.File, Idx, buffer)
	if err != nil {
		fs.log.Error("[QFS:%s:%d] Cannot append to datafile {%d} Error: %s", fs.name, Idx, file.FileIdx, err.Error())
		return InvalidIdx, err
	}
	fs.log.Trace("[QFS:%s:%d] Appended to datafile {%d} ", fs.name, Idx, file.FileIdx)

	tmp = indexRecord{
		FileIndex:  file.FileIdx,
		FileOffset: uint32(offset),
		Length:     int32(len(buffer)),
		ID:         Idx,
	}
	switch option {
	case putRecordAsNew:
	case putRecordAsProcessedWithError:
		tmp.TryCount = 1
		tmp.LastAction = time.Since(startTime)
	case putRecordAsInProcess:
		tmp.State = stateInProcess
	}
	fs.idxMutex.Lock()
	intIdx := Idx - fs.MinIndex
	fs.idx[intIdx] = tmp
	fs.freeCounts[file.FileIdx]++
	fs.log.Trace("[QFS:%s] Appended to datafile {%d} count %d ", fs.name, file.FileIdx, fs.freeCounts[file.FileIdx])
	fs.checkFlush()
	fs.idxMutex.Unlock()
	return Idx, nil
}

// Put puts buffer to disk storage with setted timeout
func (fs *fileStorage) Put(buffer []byte) (StorageIdx, error) {
	return fs.put(buffer, putRecordAsNew)
}

// createStorage creates new file storage
func createStorage(StorageName, StorageLocation string, Log Logging, Options *StorageOptions,
	TimeOut time.Duration, Notity newMessageNotificator) (*fileStorage, error) {
	if Log == nil {
		z := nullLog(0)
		Log = z
	}
	Log.Info("[fileStorage][%s] is created...", StorageName)
	if Options == nil {
		Options = &DefaultStorageOptions
	}
	tmp := &fileStorage{
		folder:                    normalizeFilePath(StorageLocation),
		name:                      StorageName,
		log:                       Log,
		options:                   Options,
		freeCounts:                make(map[StorageIdx]int64),
		readFiles:                 make(map[StorageIdx]fileAccess),
		lastTimeCheckDeletedFiles: time.Since(startTime),
		timeout:                   TimeOut,
		notify:                    Notity,
	}
	tmp.writeFiles = createIOQueue(tmp)
	err := tmp.loadIndexFile()
	if err != nil {
		tmp.log.Error("[fileStorage][%s] Cannot create:%s", StorageName, err.Error())
		err = tmp.restoreIndexFile()
		if err != nil {
			tmp.log.Error("[fileStorage][%s] Cannot restore:%s", StorageName, err.Error())
			return nil, err
		}
	}
	Log.Info("[fileStorage][%s] was created successful...", StorageName)
	return tmp, nil
}

// close closes all interhal opened handles
func (fs *fileStorage) close() {
	if fs.mmapinfo != nil {
		fs.mmapinfo.Unmap()
		fs.idxFile.Close()
		fs.mmapinfo = nil
		fs.idxFile = nil
	}
	fs.readMutex.Lock()
	for _, k := range fs.readFiles {
		k.Lock()
		k.Handle.Close()
		k.Unlock()
	}
	fs.readFiles = nil
	fs.readMutex.Unlock()
	fs.writeFiles.free()
}

// Close closes file storage
func (fs *fileStorage) Close() {
	fs.log.Info("[QFS:%s] is closed... Record count is %d", fs.name, fs.TotalRecord-fs.TotalFree)
	fs.checkFreeIndexRecords(false)
	fs.close()
	fs.checkUnusedFiles()
	fs.log.Info("[QFS:%s] was closed successful...", fs.name)
	return
}

func (fs *fileStorage) info() {
	fs.idxMutex.Lock()
	defer fs.idxMutex.Unlock()
	Now := time.Since(startTime)
	fs.log.Info("[QFS:%s]\n %v", fs.name, fs.indexFileHeader)
	for i := uint64(0); i < fs.TotalRecord; i++ {
		s := ""
		if fs.idx[i].State == stateEnable && fs.idx[i].TryCount > 0 {
			d := fs.idx[i].LastAction + time.Duration(fs.idx[i].TryCount)*time.Duration(fs.options.SkipDelayPerTry) - Now
			s = "Available in:" + d.String()
		}
		fs.log.Info("[QFS:%s:%d] %v  %s", fs.name, i+uint64(fs.MinIndex), fs.idx[i], s)
	}
}
