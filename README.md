[![Go Report Card](https://goreportcard.com/badge/github.com/sybrexsys/RapidMQ)](https://goreportcard.com/report/github.com/sybrexsys/RapidMQ)
[![Build Status](https://travis-ci.org/sybrexsys/RapidMQ.svg?branch=master)](https://travis-ci.org/sybrexsys/RapidMQ)
[![Coverage Status](https://coveralls.io/repos/github/sybrexsys/RapidMQ/badge.svg?branch=master)](https://coveralls.io/github/sybrexsys/RapidMQ?branch=master)
[![GoDoc](https://godoc.org/github.com/sybrexsys/RapidMQ?status.png)](https://godoc.org/github.com/sybrexsys/RapidMQ)


RapidMQ
=======
RapidMQ is a pure, extremely productive, lightweight and reliable library for managing of the local messages queue in the [Go programming language](http:golang.org).       

Installation
-----------

	go get github.com/sybrexsys/RapidMQ/queue

Requirements
-----------

* Need at least `go1.4` or newer.

Usage
-----------

***Queue***

Base structure in the base is Queue
Queue is created with that function:

```
func CreateQueue(Name, StoragePath string, Log Logging, Factory WorkerFactory, Options *Options) (*Queue, error)
```

|Parameters         | Type         | Description
|:----------------- |:-------------|:---------------------- 
|Name 	            |string        | Queue name. Used for logging only
|StoragePath        |string        | Path to the disk storages' files
|Log 			    |Logging 	   | Interface is used to logging of the queue's events. If equal to nil, logging is ensent. Description bellow
|Factory 			|WorkerFactory | Interface for abstract factory of the workers. Description bellow 
|Options 			|*Options      | Options of the queue

```
func (q *Queue) Insert(buf []byte) bool
```
Appends the message into the queue. In depends of the timeout's option either is trying to write message to the disk or is trying to process this message in the memory and writing to the disk only if timeout is expired shortly. Returns false if aren't processing / writing of the message in the during of the timeout or has some problems with  writing to disk    
 
```
func (q *Queue) Process(worker WorkerID, isOk bool)
``` 
That function must be called from the worker of the message. In depends of the `isOk` parameter either messages are deleting from the queue or are marking as faulty and again processing after some timeout     

```
func (q *Queue) Count() uint64
``` 
Returns the count of the messages in the queue

```
func (q *Queue) Close()
``` 
Stops the handler of the messages, saves the messages located in the memory into the disk, closes all opened files.               

***Message***

Description of the structure that will be sent to worker 

```
type Message struct {
	ID      StorageIdx
	Buffer  []byte
}
```

|Member             | Type         | Description
|:----------------- |:-------------|:---------------------- 
| ID 	            | StorageIdx   | ID of the message
| Buffer            |[]byte        | Buffer with content of the message




***WorkerFactory***

Worker factory is a structure that create workers for processing messages
Your factory must support next interface: 
```
type WorkerFactory interface {
	CreateWorker() Worker
	NeedTimeoutProcessing() bool
}
```

```
CreateWorker() Worker
```
Creates new worker for this factory with unique ID

```
NeedTimeoutProcessing() bool
```
Returns true if possible used some messages in one action (for example, collect large SQL script from lot of the small messages)  



***Worker***

If you are using of your worker, he must support next interface
```
type Worker interface {
	ProcessMessage(*Queue, *Message, chan Worker)
	ProcessTimeout(*Queue, chan Worker)
	GetID() WorkerID
	Close()
}
```

```
ProcessMessage(*Queue, *Message, chan Worker)
``` 
Processes message that is stored in `*Message`.
After it the worker must call function `(*Queue).Process` with his unique identifier and with result of the processing, also must be pushed himself into chanal `Worker`

```
ProcessTimeout(*Queue, chan Worker)
```
Processing of the event when available messages is absent   
After it the worker must call function `(*Queue).Process` with his unique identifier and with result of the processing, also must send himself into chanal `Worker`

```
GetID() WorkerID
```
Returns unique identifier of the worker

```
Close() 
```
Close is called when queue is finishing work with worker. Here you can close connection to database or etc.


***Logging***

If you are using of your logging system, it must support next interface

``` 
type Logging interface {
	Trace(msg string, a ...interface{})
	Info(msg string, a ...interface{})
	Warning(msg string, a ...interface{})
	Error(msg string, a ...interface{})
} 
```
 
 

Author
------
  ***Vadim Shakun:***  [vadim.shakun@gmail.com](mailto:vadim.shakun@gmail.com)

License
-------
RapidMQ is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.
