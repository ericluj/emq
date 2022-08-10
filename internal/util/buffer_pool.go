package util

import (
	"bytes"
	"sync"
)

var bp sync.Pool

func init() {
	bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

func BufferPoolGet() *bytes.Buffer {
	return bp.Get().(*bytes.Buffer)
}

func BufferPoolPut(b *bytes.Buffer) {
	b.Reset()
	bp.Put(b)
}
