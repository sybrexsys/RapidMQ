package queue

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path"
	"strconv"
)

const magicNumberValue = 0xEFCDAB8967452301
const magicNumberDataValue = 0x0123456789ABCDEF
const magicNumberIsFile = 0xDEADDADAFCDBACAB

// dataFileNameByID creates filename by index of the record
func dataFileNameByID(ID StorageIdx) string {
	const zeros string = "000000"
	var length int
	if ID&0xFFFF == ID {
		length = 4
	} else if ID&0xFFFFFFFF == ID {
		length = 8
	} else if ID&0xFFFFFFFFFFFF == ID {
		length = 12
	} else {
		length = 16
	}
	length++
	str := strconv.FormatUint(uint64(ID), 16)
	return "stg" + zeros[1:length-len(str)+1] + str + ".dat"
}

// checkValidFileDataName checks validity of the filename
func checkValidFileDataName(name string) int64 {
	ext := path.Ext(name)
	if ext != ".dat" {
		return -1
	}
	fname := path.Base(name)
	fname = fname[:len(fname)-len(ext)]
	if len(fname) < 3 {
		return -1
	}
	if fname[:3] != "stg" {
		return -1
	}
	fname = fname[3:]
	res, err := strconv.ParseInt(fname, 16, 0)
	if err != nil {
		return -1
	}
	return res
}

// saveDataPrefix saves to data file information about data record (prefix and size of the record)
func saveDataPrefix(fs *os.File, index StorageIdx, size int32) error {
	var valAll [16]byte
	binary.LittleEndian.PutUint32(valAll[:], magicNumberDataPrefix)
	binary.LittleEndian.PutUint64(valAll[4:], uint64(index))
	binary.LittleEndian.PutUint32(valAll[12:], uint32(size))
	_, err := fs.Write(valAll[:])
	return err
}

//saveDataSuffix Saves suffix of the data record
func saveDataSuffix(fs *os.File, crc uint32) error {
	var val [8]byte
	binary.LittleEndian.PutUint32(val[:], crc)
	binary.LittleEndian.PutUint32(val[4:], magicNumberDataSuffix)
	_, err := fs.Write(val[:])
	return err
}

//saveDataFileHeader saves identity information about data file
func saveDataFileHeader(fs *os.File) error {
	var val [8]byte
	binary.LittleEndian.PutUint64(val[:], uint64(magicNumberDataValue))
	_, err := fs.Write(val[:])
	return err
}

func saveDataFileData(File *os.File, Idx StorageIdx, buffer []byte) error {
	crc := crc32.ChecksumIEEE(buffer)
	if err := saveDataPrefix(File, Idx, int32(len(buffer))); err != nil {
		return err
	}
	if _, err := File.Write(buffer); err != nil {
		return err
	}
	return saveDataSuffix(File, crc)
}

func bufToStream(buf []byte) (io.ReadSeeker, error) {
	if len(buf) < 8 || binary.LittleEndian.Uint64(buf[:]) != uint64(magicNumberIsFile) {
		return bytes.NewReader(buf), nil
	}
	fileName := string(buf[8:])
	return os.OpenFile(fileName, os.O_RDONLY, 0666)
}

func normalizeFilePath(Path string) string {
	a := []rune(Path)
	if a[len(a)-1] == rune(os.PathSeparator) {
		return Path
	}
	return Path + string(os.PathSeparator)
}
