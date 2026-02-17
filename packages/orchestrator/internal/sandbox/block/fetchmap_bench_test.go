package block

import (
	"fmt"
	"sync"
	"testing"

	cmap "github.com/orcaman/concurrent-map/v2"
)

// Benchmarks comparing map implementations for the fetchMap in StreamingChunker.
//
// Cardinality context — concurrent fetchMap entries depend on request source:
//
//   - UFFD page faults: 4KB (regular) or 2MB (hugepage) reads. Concurrency
//     limited by vCPU count (typically 1-2). Each read hits one 4MB chunk,
//     so ~2 sessions.
//   - Prefetcher: 16 fetch workers + 8 copy workers, each calling Slice
//     with blockSize. Worst case all hit different chunks → ~24 sessions.
//   - NBD: Overlay.ReadAt breaks requests into blockSize (4KB) chunks,
//     each calling StreamingChunker.ReadAt. Largely sequential → ~1-3 sessions.
//
// Realistic peak is ~16-24 concurrent sessions (prefetcher burst). We bench
// at 4, 16, and 64 entries to cover typical, peak, and worst-case.

type fetchMapImpl interface {
	get(key int64) (*fetchSession, bool)
	set(key int64, v *fetchSession)
	del(key int64)
}

// --- mutex + map[int64] (current implementation) ---

type mutexMap struct {
	mu sync.Mutex
	m  map[int64]*fetchSession
}

func newMutexMap() fetchMapImpl {
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

func newCmapStringMap() fetchMapImpl {
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

// --- Table-driven benchmarks ---

var mapImpls = []struct {
	name string
	new  func() fetchMapImpl
}{
	{"MutexMap", newMutexMap},
	{"CmapString", newCmapStringMap},
}

// Cardinalities matching real request patterns:
//
//	1  = single UFFD fault or sequential NBD read (dominant case)
//	4  = UFFD faults + NBD (typical, 1-2 vCPUs)
//	16 = prefetcher fetch workers (default MemoryPrefetchMaxFetchWorkers)
//	64 = worst case (prefetcher + UFFD + NBD all hitting different chunks)
var cardinalities = []int{1, 4, 16, 64}

func BenchmarkFetchMap_Serial(b *testing.B) {
	session := &fetchSession{}
	for _, impl := range mapImpls {
		for _, n := range cardinalities {
			b.Run(fmt.Sprintf("%s/%d_keys", impl.name, n), func(b *testing.B) {
				m := impl.new()
				for i := range b.N {
					key := int64(i % n)
					m.set(key, session)
					m.get(key)
					m.del(key)
				}
			})
		}
	}
}

func BenchmarkFetchMap_Parallel(b *testing.B) {
	session := &fetchSession{}
	for _, impl := range mapImpls {
		for _, n := range cardinalities {
			b.Run(fmt.Sprintf("%s/%d_keys", impl.name, n), func(b *testing.B) {
				m := impl.new()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						key := int64(i % n)
						m.set(key, session)
						m.get(key)
						m.del(key)
						i++
					}
				})
			})
		}
	}
}

func BenchmarkFetchMap_ReadHeavy(b *testing.B) {
	session := &fetchSession{}
	for _, impl := range mapImpls {
		for _, n := range cardinalities {
			b.Run(fmt.Sprintf("%s/%d_keys", impl.name, n), func(b *testing.B) {
				m := impl.new()
				for i := range int64(n) {
					m.set(i, session)
				}
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						key := int64(i % n)
						if i%10 == 0 {
							m.set(key, session)
						} else {
							m.get(key)
						}
						i++
					}
				})
			})
		}
	}
}
