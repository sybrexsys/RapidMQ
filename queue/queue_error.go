package queue

import (
	"fmt"
	"time"
)

const (
	errorNoMore = iota
	errorInDelay
	errorOverSize
	errorOverCount
	errorTimeOut
)

type queueError struct {
	ErrorType     int32
	NextAvailable time.Duration
}

func (e *queueError) Error() string {
	return fmt.Sprintf("queue internal error #%d (%v)", e.ErrorType, e.NextAvailable)
}
