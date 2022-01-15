package batch

import (
	"context"
	"fmt"
	"github.com/vsalavatov/bsse-storage-systems/keyvalue"
	"time"
)

const kRequestsBufferSize = 256
const kRotationPeriodSeconds = 15

const (
	kPUT = iota
	kGET
)

type kvRequest struct {
	reqType  uint8
	key      keyvalue.Key
	value    keyvalue.Value
	callback func(keyvalue.Value, error)
}

type kvResponse struct {
	value keyvalue.Value
	err   error
}

type BatchKeyValueProcessor struct {
	kvs          *keyvalue.PersistentKeyValueStorage
	lastRotation time.Time

	requests chan kvRequest
	ctx      context.Context
}

func NewBatchKeyValueProcessor(ctx context.Context, kvs *keyvalue.PersistentKeyValueStorage) *BatchKeyValueProcessor {
	b := BatchKeyValueProcessor{
		kvs:          kvs,
		lastRotation: time.Now(),
		requests:     make(chan kvRequest, kRequestsBufferSize),
		ctx:          ctx,
	}
	go b.run()
	return &b
}

func (bp *BatchKeyValueProcessor) run() {
	for {
		select {
		case <-bp.ctx.Done():
			return
		case req := <-bp.requests:
			requests := make([]kvRequest, 0, kRequestsBufferSize)
			requests = append(requests, req)
			for len(requests) < kRequestsBufferSize { // read all available requests
				select {
				case req := <-bp.requests:
					requests = append(requests, req)
				default:
					goto processBatch
				}
			}
		processBatch:
			responses := make([]kvResponse, len(requests))
			anyPut := false
			for i := 0; i < len(requests); i++ {
				switch requests[i].reqType {
				case kPUT:
					responses[i].err = bp.kvs.Put(requests[i].key, requests[i].value)
					anyPut = true
				case kGET:
					responses[i].value, responses[i].err = bp.kvs.Get(requests[i].key)
				default:
					panic(fmt.Sprint("unexpected persistent key-value request type:", requests[i].reqType))
				}
			}
			if anyPut {
				if err := bp.kvs.Sync(); err != nil {
					panic(err)
				}
			}
			for i := 0; i < len(requests); i++ {
				requests[i].callback(responses[i].value, responses[i].err)
			}
			if bp.lastRotation.Add(kRotationPeriodSeconds * time.Second).Before(time.Now()) {
				available, err := bp.kvs.IsRotationAvailable()
				if err != nil {
					panic(fmt.Sprint("hashtable rotation failure:", err.Error()))
				}
				if available {
					bp.lastRotation = time.Now()
					bp.kvs.Rotate()
				}
			}
		}
	}
}

func (bp *BatchKeyValueProcessor) Put(key keyvalue.Key, value keyvalue.Value, callback func(error)) {
	bp.requests <- kvRequest{
		reqType: kPUT,
		key:     key,
		value:   value,
		callback: func(_ keyvalue.Value, err error) {
			callback(err)
		},
	}
}

func (bp *BatchKeyValueProcessor) Get(key keyvalue.Key, callback func(keyvalue.Value, error)) {
	bp.requests <- kvRequest{
		reqType:  kGET,
		key:      key,
		callback: callback,
	}
}
