package queue

import "time"

// StorageOptions holds the optional parameters for the disk storage of the messages.
type StorageOptions struct {
	// maximum size of the storage's data files
	MaxDataFileSize int64

	// Count of the operation with storage when index file will be flushed
	FlushOperations uint32

	// Count of the percents if the free messages before close of the when index file will be reformed
	PercentFreeForRecalculateOnExit uint8

	// Count of the percents if the free messages when index file will be reformed
	PercentFreeForRecalculateOnIncrementIndexFile uint8

	// Depends skip error messages if timeout of the waiting did not finished yet
	SkipReturnedRecords bool

	// Duration of the timeout. Time of the next processing calculated by TimeOfError+CountOfTheErrors*SkipDelayPerTry
	SkipDelayPerTry uint32

	// Depends check crc of the message before sent in to worker
	CheckCRCOnRead bool

	// Count of the one time opened for reading and for writing files. Open files are counting separately
	MaxOneTimeOpenedFiles int16

	// If queue index file is corrupted then will recreate index file and try to restore ,essages information
	DeleteInvalidIndexFile bool
}

//Options holds the optional parameters for the managing of the messages.
type Options struct {
	// Options for file storage connected to this queue
	StorageOptions *StorageOptions

	// In the during of timeout, message must be processed or saved to disk
	InputTimeOut time.Duration

	// Maximum size of the messages what can be processed without storing to disk
	MaximumQueueMessagesSize int32

	// Maximum count of the messages what can be processed without storing to disk
	MaximumMessagesInQueue uint16

	// Minimum count of the workers per queue
	MinimunWorkersCount uint16

	// Minimum count of the workers per queue
	MaximumWorkersCount uint16

	// Maximum count of the messages that thw worker can crocess per one time
	MaximumMessagesPerWorker uint16
}

// DefaultStorageOptions is default options for filestorage
var DefaultStorageOptions = StorageOptions{
	MaxDataFileSize:                               0x1FFFFFFF, //
	FlushOperations:                               512,
	PercentFreeForRecalculateOnExit:               5,
	PercentFreeForRecalculateOnIncrementIndexFile: 10,
	SkipReturnedRecords:                           true,
	SkipDelayPerTry:                               500,
	CheckCRCOnRead:                                false,
	MaxOneTimeOpenedFiles:                         12,
	DeleteInvalidIndexFile:                        true,
}

// DefaultQueueOptions is default options for queue
var DefaultQueueOptions = Options{
	MinimunWorkersCount:      4,
	MaximumWorkersCount:      32,
	StorageOptions:           nil,
	MaximumMessagesPerWorker: 2048,
	InputTimeOut:             5 * time.Second,
	MaximumMessagesInQueue:   2048,
	MaximumQueueMessagesSize: 16 * 1024 * 1024,
}
