package queue

type Logging interface {
	Trace(msg string, a ...interface{})
	Info(msg string, a ...interface{})
	Warning(msg string, a ...interface{})
	Error(msg string, a ...interface{})
}

type nullLog int

func (logger nullLog) Trace(msg string, a ...interface{}) {
}

func (logger nullLog) Warning(msg string, a ...interface{}) {
}

func (logger nullLog) Error(msg string, a ...interface{}) {
}

func (logger nullLog) Info(msg string, a ...interface{}) {
}
