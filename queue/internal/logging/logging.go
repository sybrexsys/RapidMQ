package logging

import (
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
)


const (
	logComMessage = iota
	logComClose
)

const (
	logInfoTypeError = iota
	logInfoTypeWarning
	logInfoTypeInfo
	logInfoTypeTrace
)

type logMessage struct {
	command int
	id      int
	message string
}

type Logger struct {
	chanal      chan logMessage
	isOuted     chan struct{}
	file        *os.File
	canwrite    int32
	fileName    string
	maxFileSize int64
	level       byte
}

func addPrefix(infotype int, msg string, a ...interface{}) string {
	var m string
	switch infotype {
	case logInfoTypeError:
		m = " [E] " + msg
	case logInfoTypeWarning:
		m = " [W] " + msg
	case logInfoTypeTrace:
		m = " [T] " + msg
	default:
		m = " [I] " + msg
	}
	return fmt.Sprintf(m, a...)
}

func intToStrWithZero(Num, count int) string {
	const zeros string = "000000000000000000000000000000"
	str := strconv.Itoa(Num)
	return zeros[1:count-len(str)+1] + str
}

func (logger *Logger) startRotation() {
	for {
		//time.Sleep(100)
		data, ok := <-logger.chanal
		if !ok || data.command == logComClose {
			break
		}
		logger.saveMessage(data.message, data.id)
	}
	logger.isOuted <- struct{}{}
}

func (logger *Logger) saveHeader() {
	writeStrToFile(logger.file, 0, "")
	writeStrToFile(logger.file, 10, "----- Started -----")
	writeStrToFile(logger.file, 100, fmt.Sprintf("NumCPU:[%d] OS:[%s] Arch:[%s]", runtime.NumCPU(), runtime.GOOS, runtime.GOARCH))
	writeStrToFile(logger.file, 1000, "")
	writeStrToFile(logger.file, 10000, "-------------------")
}

func writeStrToFile(file *os.File, id int, message string) error {
	str := time.Now().Format("2006-01-02 15:04:05.000") + message + "\n"
	buffer := []byte(str)
	if _, err := file.Write(buffer); err != nil {
		return err
	}
	return nil
}

func (logger *Logger) saveMessage(msg string, id int) error {
	pos, err := logger.file.Seek(0, 2)
	if err != nil {
		return err
	}
	if pos > logger.maxFileSize {
		logger.file.Close()
		ext := path.Ext(logger.fileName)
		bakfile := logger.fileName[0:len(logger.fileName)-len(ext)] + ".bak"
		if err = os.Remove(bakfile); err == nil || os.IsNotExist(err) {
			os.Rename(logger.fileName, bakfile)
		}
		logger.file, err = os.OpenFile(logger.fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			atomic.StoreInt32(&logger.canwrite, 0)
			return err
		}
		logger.saveHeader()
	}

	if err = writeStrToFile(logger.file, id, msg); err != nil {
		atomic.StoreInt32(&logger.canwrite, 0)
		return err
	}
	return nil
}

func CreateLog(fileName string, maxFileSize int64, level byte) (logger *Logger, err error) {
	logger = new(Logger)
	logger.fileName = fileName
	logger.maxFileSize = maxFileSize
	logger.level = level
	logger.canwrite = 0
	logger.chanal = make(chan logMessage, 1000)
	logger.isOuted = make(chan struct{})
	err = nil
	logger.file, err = os.OpenFile(logger.fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	atomic.StoreInt32(&logger.canwrite, 1)
	logger.saveHeader()
	go logger.startRotation()
	return
}

func (logger *Logger) infoout(infotype, id int, msg string, a ...interface{}) {
	m := addPrefix(infotype, msg, a...)
	if logger == nil {
		log.Println(m)
		return
	}
	if atomic.AddInt32(&logger.canwrite, 0) == 0 {
		return
	}
	logger.chanal <- logMessage{
		command: logComMessage,
		message: m,
		id:      id,
	}
}

func (logger *Logger) Trace(msg string, a ...interface{}) {
	logger.infoout(logInfoTypeTrace, 0, msg, a...)
}

func (logger *Logger) Warning(msg string, a ...interface{}) {
	logger.infoout(logInfoTypeWarning, 0, msg, a...)
}

func (logger *Logger) Error(msg string, a ...interface{}) {
	logger.infoout(logInfoTypeError, 0, msg, a...)
}

func (logger *Logger) Info(msg string, a ...interface{}) {
	logger.infoout(logInfoTypeInfo, 0, msg, a...)
}

func (logger *Logger) TraceID(id int, msg string) {
	logger.infoout(logInfoTypeTrace, id, msg)
}

func (logger *Logger) WarningID(id int, msg string) {
	logger.infoout(logInfoTypeWarning, id, msg)
}

func (logger *Logger) ErrorID(id int, msg string) {
	logger.infoout(logInfoTypeError, id, msg)
}

func (logger *Logger) InfoID(id int, msg string) {
	logger.infoout(logInfoTypeInfo, id, msg)
}

func (logger *Logger) Close() {

	logger.chanal <- logMessage{
		command: logComClose,
		message: "",
	}
	<-logger.isOuted
	logger.file.Close()

}

type StdLog int

func (logger StdLog) Trace(msg string, a ...interface{}) {
	m := addPrefix(logInfoTypeTrace, msg, a...)
	log.Println(m)
}

func (logger StdLog) Warning(msg string, a ...interface{}) {
	m := addPrefix(logInfoTypeWarning, msg, a...)
	log.Println(m)
}

func (logger StdLog) Error(msg string, a ...interface{}) {
	m := addPrefix(logInfoTypeError, msg, a...)
	log.Println(m)
}

func (logger StdLog) Info(msg string, a ...interface{}) {
	m := addPrefix(logInfoTypeInfo, msg, a...)
	log.Println(m)
}
