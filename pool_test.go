package sftp

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResChanPoolReusesChannels(t *testing.T) {
	pool := newResChanPool(1)
	ch := pool.Get()
	pool.Put(ch)

	assert.Equal(t, ch, pool.Get())
}

func TestResChanPoolConcurrentUse(t *testing.T) {
	const workers = 32

	pool := newResChanPool(workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for range 100 {
				ch := pool.Get()
				ch <- result{typ: sshFxpStatus}
				got := <-ch
				assert.Equal(t, fxp(sshFxpStatus), got.typ)
				pool.Put(ch)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkResChanPool(b *testing.B) {
	const depth = 64

	cycle := func(pool resChanPool) {
		var channels [depth]chan result
		for i := range channels {
			channels[i] = pool.Get()
		}

		for _, ch := range channels {
			pool.Put(ch)
		}
	}

	b.Run("per-operation", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			cycle(newResChanPool(depth))
		}
	})

	b.Run("shared", func(b *testing.B) {
		pool := newResChanPool(depth)
		cycle(pool)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			cycle(pool)
		}
	})
}
