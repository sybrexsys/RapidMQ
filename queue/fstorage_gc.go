package queue

import (
	"errors"
	"os"
	"sync/atomic"
)

type calcsize struct {
	cnt      int
	size     int64
	realsize int64
}

type garbageCollectInfo struct {
	fileIndex StorageIdx
	recCount  int64
	file      *fileAccess
}

//	actions:
// 1. You must find all files where size of useful space is not more then 10 per cent
//    of total size of the file
//    Opened files is not processing

//  after processing delete files from  opended for reading files list

func (fs *fileStorage) garbageCollect() error {
	fs.idxMutex.Lock()
	defer func() {
		fs.idxMutex.Unlock()
		fs.log.Trace("[Q:%s] Garbage collection finished", fs.name)
	}()
	atomic.StoreInt32(&fs.immediatlyRelease, 0)
	if fs.TotalRecord == 0 {
		return nil
	}
	fs.log.Trace("[Q:%s] Garbage collection is started", fs.name)
	needProcess := make(map[StorageIdx]*calcsize)
	for k := range fs.freeCounts {
		fileinfo, err := os.Stat(fs.folder + dataFileNameByID(k))
		if err != nil {
			continue
		}
		needProcess[k] = &calcsize{
			cnt:      0,
			size:     0,
			realsize: fileinfo.Size() / 10,
		}
	}
	// Calculate list of the files where count of the valid records bellow then 10 % of size of file
	for i := uint64(0); i < fs.TotalRecord; i++ {
		if fs.idx[i].State != stateEnable {
			continue
		}
		fileidx := fs.idx[i].FileIndex
		cur, ok := needProcess[fileidx]
		if !ok {
			continue
		}
		cur.cnt++
		cur.size += int64(fs.idx[i].Length)
		if cur.realsize < cur.size {
			delete(needProcess, fileidx)
		}
	}
	if len(needProcess) == 0 {
		return nil
	}
	gi := &garbageCollectInfo{
		fileIndex: InvalidIdx,
		recCount:  0,
		file:      &fileAccess{},
	}

	// Move valid records into new file
	for i := uint64(0); i < fs.TotalRecord; i++ {
		if fs.idx[i].State != stateEnable {
			continue
		}
		fileidx := fs.idx[i].FileIndex
		_, ok := needProcess[fileidx]
		if !ok {
			continue
		}
		if atomic.LoadInt32(&fs.immediatlyRelease) == 1 {
			break
		}
		if err := fs.moveOneRecord(i, gi); err != nil {
			return err
		}
	}
	write := &fileWrite{
		File:    gi.file.Handle,
		FileIdx: gi.fileIndex,
	}
	fs.writeFiles.putHandle(write, nil)
	fs.freeCounts[gi.fileIndex] = gi.recCount
	return nil
}

func (fs *fileStorage) getNextFreeIndex() StorageIdx {
	for i := StorageIdx(0); i < fs.MinIndex+StorageIdx(i); i++ {
		if _, ok := fs.freeCounts[i]; !ok {
			return i
		}
	}
	return InvalidIdx
}

func (fs *fileStorage) moveOneRecord(idx uint64, gi *garbageCollectInfo) error {
	nidx := StorageIdx(InvalidIdx)
	if gi.file.Handle == nil {
		nidx = fs.getNextFreeIndex()
	} else {
		sz, err := gi.file.Handle.Seek(0, 2)
		if err != nil {
			return err
		}
		if sz+int64(fs.idx[idx].Length) > fs.options.MaxDataFileSize {

			nidx = fs.getNextFreeIndex()
		}
	}
	if nidx != InvalidIdx {
		if gi.fileIndex != InvalidIdx {
			fs.freeCounts[gi.fileIndex] = gi.recCount
			gi.file.Handle.Close()
		}
		gi.fileIndex = nidx
		datFileName := fs.folder + dataFileNameByID(nidx)
		f, err := os.Create(datFileName)
		if err != nil {
			fs.log.Error("[FSQ:%s] Cannot create datafile {%d} Error: %s", fs.name, idx, err.Error())
			return err
		}
		fs.log.Trace("[FSQ:%s] Create datafile {%d}", fs.name, idx)
		saveDataFileHeader(f)
		gi.file.Handle = f
		gi.recCount = 0
	}
	oldFileIdx := fs.idx[idx].FileIndex
	fl, err := fs.getReadHandle(fs.idx[idx].ID, oldFileIdx)
	if err != nil {
		return err
	}
	data := fs.getValidRecord(fl, fs.idx[idx].ID, fs.idx[idx].FileOffset, fs.idx[idx].Length)
	if data == nil {
		return errors.New("empty data was found")
	}

	offset, err := gi.file.Handle.Seek(0, 2)
	if err != nil {
		fs.log.Error("[QFS:%s:%d] Cannot move to datafile {%d} Error: %s", fs.name, fs.idx[idx].ID, gi.fileIndex, err.Error())
		return err
	}
	err = saveDataFileData(gi.file.Handle, fs.idx[idx].ID, data)
	if err != nil {
		fs.log.Error("[QFS:%s:%d] Cannot append to datafile {%d} Error: %s", fs.name, fs.idx[idx].ID, gi.fileIndex, err.Error())
		return err
	}
	fs.idx[idx].FileIndex = gi.fileIndex
	fs.idx[idx].FileOffset = uint32(offset)
	gi.recCount++
	oldfileCnt := fs.freeCounts[oldFileIdx]
	oldfileCnt--
	fs.freeCounts[oldFileIdx] = oldfileCnt
	if oldfileCnt == 0 {
		delete(fs.freeCounts, oldFileIdx)
		m := fs.readFiles[oldFileIdx]
		m.Handle.Close()
		delete(fs.readFiles, oldFileIdx)
		os.Remove(fs.folder + dataFileNameByID(oldFileIdx))
	}
	return nil
}
