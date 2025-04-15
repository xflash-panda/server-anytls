package service

import (
	"sync/atomic"
)

type Counter struct {
	num uint64
}

func NewCounter(num uint64) *Counter {
	return &Counter{0}
}

func (c *Counter) Add(value uint64) {
	atomic.AddUint64(&c.num, value)
}

func (c *Counter) Reset() {
	atomic.StoreUint64(&c.num, 0)
}

func (c *Counter) Value() uint64 {
	return atomic.LoadUint64(&c.num)
}
