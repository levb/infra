package block

import (
	"fmt"
	"sync"
	"testing"

	cmap "github.com/orcaman/concurrent-map/v2"
)

// Benchmarks comparing map implementations for the fetchMap in StreamingChunker.
// The fetchMap holds ~1-4 entries (one per in-flight 4MB chunk fetch), so the
// dominant cost is synchronization overhead, not map operations themselves.

const benchEntries = 4

// --- mutex + map[int64] (current implementation) ---

type mutexMap struct {
	mu sync.Mutex
	m  map[int64]*fetchSession
}

func newMutexMap() *mutexMap {
	return &mutexMap{m: make(map[int64]*fetchSession)}
}

func (mm *mutexMap) get(key int64) (*fetchSession, bool) {
	mm.mu.Lock()
	v, ok := mm.m[key]
	mm.mu.Unlock()
	return v, ok
}

func (mm *mutexMap) set(key int64, v *fetchSession) {
	mm.mu.Lock()
	mm.m[key] = v
	mm.mu.Unlock()
}

func (mm *mutexMap) del(key int64) {
	mm.mu.Lock()
	delete(mm.m, key)
	mm.mu.Unlock()
}

// --- concurrent-map with string keys ---

type cmapStringMap struct {
	m cmap.ConcurrentMap[string, *fetchSession]
}

func newCmapStringMap() *cmapStringMap {
	return &cmapStringMap{m: cmap.New[*fetchSession]()}
}

func (cm *cmapStringMap) get(key int64) (*fetchSession, bool) {
	return cm.m.Get(fmt.Sprintf("%d", key))
}

func (cm *cmapStringMap) set(key int64, v *fetchSession) {
	cm.m.Set(fmt.Sprintf("%d", key), v)
}

func (cm *cmapStringMap) del(key int64) {
	cm.m.Remove(fmt.Sprintf("%d", key))
}

// --- Benchmarks ---

func BenchmarkFetchMap_Serial(b *testing.B) {
	session := &fetchSession{}

	b.Run("MutexMap", func(b *testing.B) {
		mm := newMutexMap()
		for i := 0; i < b.N; i++ {
			key := int64(i % benchEntries)
			mm.set(key, session)
			mm.get(key)
			mm.del(key)
		}
	})

	b.Run("CmapString", func(b *testing.B) {
		cm := newCmapStringMap()
		for i := 0; i < b.N; i++ {
			key := int64(i % benchEntries)
			cm.set(key, session)
			cm.get(key)
			cm.del(key)
		}
	})
}

func BenchmarkFetchMap_Parallel(b *testing.B) {
	session := &fetchSession{}

	b.Run("MutexMap", func(b *testing.B) {
		mm := newMutexMap()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := int64(i % benchEntries)
				mm.set(key, session)
				mm.get(key)
				mm.del(key)
				i++
			}
		})
	})

	b.Run("CmapString", func(b *testing.B) {
		cm := newCmapStringMap()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := int64(i % benchEntries)
				cm.set(key, session)
				cm.get(key)
				cm.del(key)
				i++
			}
		})
	})
}

func BenchmarkFetchMap_ReadHeavy(b *testing.B) {
	session := &fetchSession{}

	b.Run("MutexMap", func(b *testing.B) {
		mm := newMutexMap()
		for i := range int64(benchEntries) {
			mm.set(i, session)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := int64(i % benchEntries)
				if i%10 == 0 {
					mm.set(key, session)
				} else {
					mm.get(key)
				}
				i++
			}
		})
	})

	b.Run("CmapString", func(b *testing.B) {
		cm := newCmapStringMap()
		for i := range int64(benchEntries) {
			cm.set(i, session)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := int64(i % benchEntries)
				if i%10 == 0 {
					cm.set(key, session)
				} else {
					cm.get(key)
				}
				i++
			}
		})
	})
}
