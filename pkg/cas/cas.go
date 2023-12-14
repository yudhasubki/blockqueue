package cas

import (
	"runtime"
	"sync/atomic"
)

type SpinLock struct {
	state *uint32
}

const free = uint32(0)

func New() *SpinLock {
	var poke *uint32
	poke = new(uint32)
	*poke = 0

	return &SpinLock{
		state: poke,
	}
}

func (l *SpinLock) Lock() {
	for !atomic.CompareAndSwapUint32(l.state, free, 1) {
		runtime.Gosched()
	}
}

func (l *SpinLock) Unlock() {
	atomic.StoreUint32(l.state, free)
}
