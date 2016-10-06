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
	message string
}

//Logger is structure for internal use
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
		logger.saveMessage(data.message)
	}
	logger.isOuted <- struct{}{}
}

func (logger *Logger) saveHeader() {
	writeStrToFile(logger.file, "")
	writeStrToFile(logger.file, "----- Started -----")
	writeStrToFile(logger.file, fmt.Sprintf("NumCPU:[%d] OS:[%s] Arch:[%s]", runtime.NumCPU(), runtime.GOOS, runtime.GOARCH))
	writeStrToFile(logger.file, "")
	writeStrToFile(logger.file, "-------------------")
}

func writeStrToFile(file *os.File, message string) error {
	str := time.Now().Format("2006-01-02 15:04:05.000") + message + "\n"
	buffer := []byte(str)
	if _, err := file.Write(buffer); err != nil {
		return err
	}
	return nil
}

func (logger *Logger) saveMessage(msg string) error {
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

	if err = writeStrToFile(logger.file, msg); err != nil {
		atomic.StoreInt32(&logger.canwrite, 0)
		return err
	}
	return nil
}

//CreateLog is created new logging system
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
	}
}

// Trace outputs the message into log with Trace level
func (logger *Logger) Trace(msg string, a ...interface{}) {
	logger.infoout(logInfoTypeTrace, 0, msg, a...)
}

// Warning outputs the message into log with Warning level
func (logger *Logger) Warning(msg string, a ...interface{}) {
	logger.infoout(logInfoTypeWarning, 0, msg, a...)
}

// Error outputs the message into log with Warning level
func (logger *Logger) Error(msg string, a ...interface{}) {
	logger.infoout(logInfoTypeError, 0, msg, a...)
}

// Info outputs the message into log with Warning level
func (logger *Logger) Info(msg string, a ...interface{}) {
	logger.infoout(logInfoTypeInfo, 0, msg, a...)
}

// Close closes all opened handles and stop logging
func (logger *Logger) Close() {

	logger.chanal <- logMessage{
		command: logComClose,
		message: "",
	}
	<-logger.isOuted
	logger.file.Close()

}
