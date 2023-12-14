package cas

import (
	"sync"
	"testing"
)

func benchRoutine(b *testing.B, size int, fn func()) {
	wg := sync.WaitGroup{}
	wg.Add(size)
	for i := 0; i < size; i++ {
		go func() {
			for i := 0; i < b.N; i++ {
				fn()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkCas10000(b *testing.B) {
	routineSize := 10000
	b.Run("run with cas", func(b *testing.B) {
		var t = struct {
			counter int
			cas     *SpinLock
		}{}

		t.cas = New()

		benchRoutine(b, routineSize, func() {
			t.cas.Lock()
			t.counter++
			t.cas.Unlock()
		})

		if int64(routineSize*b.N) != int64(t.counter) {
			b.Logf("Expected %d but got %d", routineSize*b.N, t.counter)
			b.Fail()
		} else {
			b.Logf("Equal %d got %d", routineSize*b.N, t.counter)
		}
	})

	b.Run("run with mutex", func(b *testing.B) {
		var t = struct {
			counter int
			mtx     *sync.RWMutex
		}{}

		t.mtx = new(sync.RWMutex)

		benchRoutine(b, routineSize, func() {
			t.mtx.Lock()
			t.counter++
			t.mtx.Unlock()
		})
		if int64(routineSize*b.N) != int64(t.counter) {
			b.Logf("Expected %d but got %d", routineSize*b.N, t.counter)
			b.Fail()
		} else {
			b.Logf("Equal %d got %d", routineSize*b.N, t.counter)
		}
	})
}

func BenchmarkCas1000(b *testing.B) {
	routineSize := 1000
	b.Run("run with cas", func(b *testing.B) {
		var t = struct {
			counter int
			cas     *SpinLock
		}{}

		t.cas = New()

		benchRoutine(b, routineSize, func() {
			t.cas.Lock()
			t.counter++
			t.cas.Unlock()
		})

		if int64(routineSize*b.N) != int64(t.counter) {
			b.Logf("Expected %d but got %d", routineSize*b.N, t.counter)
			b.Fail()
		} else {
			b.Logf("Equal %d got %d", routineSize*b.N, t.counter)
		}
	})

	b.Run("run with mutex", func(b *testing.B) {
		var t = struct {
			counter int
			mtx     *sync.RWMutex
		}{}

		t.mtx = new(sync.RWMutex)

		benchRoutine(b, routineSize, func() {
			t.mtx.Lock()
			t.counter++
			t.mtx.Unlock()
		})
		if int64(routineSize*b.N) != int64(t.counter) {
			b.Logf("Expected %d but got %d", routineSize*b.N, t.counter)
			b.Fail()
		} else {
			b.Logf("Equal %d got %d", routineSize*b.N, t.counter)
		}
	})
}
