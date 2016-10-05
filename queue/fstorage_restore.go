package queue

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
)

func (fs *fileStorage) restoreStorageFile(FileIdx StorageIdx) error {
	var buf [16]byte
	OneRecordProcessed := false
	Handle, err := os.Open(fs.folder + dataFileNameByID(FileIdx))
	if err != nil {
		return err
	}
	defer Handle.Close()
	io.ReadFull(Handle, buf[:8])
	if binary.LittleEndian.Uint64(buf[:]) != uint64(magicNumberDataValue) {
		return errors.New("not found magic header")
	}
	for {
		offset, err := Handle.Seek(0, 1)
		if err != nil {
			break
		}
		_, err = io.ReadFull(Handle, buf[:])
		if err != nil {
			break
		}
		z := binary.LittleEndian.Uint32(buf[:])
		if z != magicNumberDataPrefix {
			break
		}
		ID := StorageIdx(binary.LittleEndian.Uint64(buf[4:]))
		length := binary.LittleEndian.Uint32(buf[12:])
		outbuf := make([]byte, length)
		_, err = io.ReadFull(Handle, outbuf)
		if err != nil {
			break
		}
		_, err = io.ReadFull(Handle, buf[:8])
		if err != nil {
			break
		}
		crc := crc32.ChecksumIEEE(outbuf)
		z = binary.LittleEndian.Uint32(buf[:])
		skip := crc != z
		z = binary.LittleEndian.Uint32(buf[4:])
		if z != magicNumberDataSuffix {
			break
		}
		if skip {
			continue
		}
		idx, err := fs.newID()
		if err != nil {
			return nil
		}
		tmp := indexRecord{
			FileIndex:  FileIdx,
			FileOffset: uint32(offset),
			Length:     int32(length),
			ID:         ID,
		}
		fs.idx[idx] = tmp
		fs.freeCounts[FileIdx]++
		OneRecordProcessed = true
	}
	if !OneRecordProcessed {
		return errors.New("not records was found")
	}
	return nil
}

// restoreIndexFile tries restore and repair index file of the storage
func (fs *fileStorage) restoreIndexFile() error {
	IsOneFileProcessed := false
	if err := fs.prepareIndexFile(); err != nil {
		return err
	}
	path := fs.folder[:len(fs.folder)-1]
	listFiles, err := ioutil.ReadDir(path)
	if err == nil {
		for _, finfo := range listFiles {
			if finfo.IsDir() {
				continue
			}
			fname := finfo.Name()
			if fname == "index.dat" {
				continue
			}
			idx := checkValidFileDataName(fname)
			if idx == -1 {
				fn := fs.folder + fname
				fs.log.Info("Remove unknown file %s", fn)
				os.Remove(fn)
				continue
			}
			err = fs.restoreStorageFile(StorageIdx(idx))
			if err != nil {
				fn := fs.folder + fname
				fs.log.Info("Remove unknown file %s", fn)
				os.Remove(fn)
				continue
			}
			IsOneFileProcessed = true
		}
	}
	if !IsOneFileProcessed {
		return errors.New("not found any valid storage file")
	}
	return nil
}

// deleteUnusedFiles deletes all files from folder which is not have linked inforation in the index file
func (fs *fileStorage) deleteUnusedFiles() {
	path := fs.folder[:len(fs.folder)-1]
	listFiles, err := ioutil.ReadDir(path)
	if err == nil {
		for _, finfo := range listFiles {
			if finfo.IsDir() {
				continue
			}
			fname := finfo.Name()
			if fname == "index.dat" {
				continue
			}
			idx := checkValidFileDataName(fname)
			if idx == -1 {
				fn := fs.folder + fname
				fs.log.Info("Remove unknown file %s", fn)
				os.Remove(fn)
				continue
			}
			_, ok := fs.freeCounts[StorageIdx(idx)]
			if !ok {
				fn := fs.folder + fname
				fs.log.Info("Remove unknown file %s", fn)
				os.Remove(fn)
			}
		}
	}
}
