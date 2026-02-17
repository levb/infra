fetchmap benchmark: mutex+map vs concurrent-map

The review suggested replacing sync.Mutex + map[int64]*fetchSession with
a sharded concurrent map (github.com/orcaman/concurrent-map). This file
documents why we kept the mutex approach.


How the fetchMap is used

The StreamingChunker maintains a fetchMap keyed by 4MB chunk offset.
getOrCreateSession does a get-or-insert, and runFetch deletes the entry
when the fetch completes. Multiple callers requesting the same chunk
share a single session, so the map stays small.

Three sources produce Slice/ReadAt calls:

  UFFD page faults — 4KB or 2MB reads, one chunk each. Concurrency
  bounded by vCPU count (typically 1-2). This is the dominant path.

  NBD — Overlay.ReadAt breaks requests into blockSize (4KB) before
  calling into the chunker. Largely sequential, ~1-3 sessions.

  Prefetcher — 16 fetch workers + 8 copy workers (feature-flag
  defaults). Worst case all hit different chunks, ~24 sessions.

The dominant case is 1 concurrent session. Realistic peak is ~16-24,
theoretical worst case ~64.


Benchmark design

Two implementations compared:
  MutexMap    — sync.Mutex + map[int64]*fetchSession (current)
  CmapString  — orcaman/concurrent-map with string keys (suggested)

Three scenarios:
  Serial    — single goroutine, set+get+delete per iteration
  Parallel  — GOMAXPROCS goroutines, set+get+delete (write-heavy)
  ReadHeavy — GOMAXPROCS goroutines, 90% gets / 10% sets

Four cardinalities: 1, 4, 16, 64 keys.


Results

```
goos: linux
goarch: amd64
cpu: AMD Ryzen 7 8845HS w/ Radeon 780M Graphics

Serial (single goroutine, set+get+delete):
  MutexMap/1_keys       41545020    27.59 ns/op    0 B/op    0 allocs/op
  CmapString/1_keys      9900007   122.3  ns/op    0 B/op    0 allocs/op
  MutexMap/4_keys       38751399    27.67 ns/op    0 B/op    0 allocs/op
  CmapString/4_keys      9647880   121.6  ns/op    0 B/op    0 allocs/op
  MutexMap/16_keys      42534451    27.92 ns/op    0 B/op    0 allocs/op
  CmapString/16_keys     9003577   132.2  ns/op    2 B/op    1 allocs/op
  MutexMap/64_keys      43147730    27.78 ns/op    0 B/op    0 allocs/op
  CmapString/64_keys     8378761   144.3  ns/op    5 B/op    2 allocs/op

Parallel (GOMAXPROCS goroutines, set+get+delete):
  MutexMap/1_keys       11516026   106.8  ns/op    0 B/op    0 allocs/op
  CmapString/1_keys      6344956   172.1  ns/op    0 B/op    0 allocs/op
  MutexMap/4_keys       10883929   107.7  ns/op    0 B/op    0 allocs/op
  CmapString/4_keys      7162670   158.1  ns/op    0 B/op    0 allocs/op
  MutexMap/16_keys      10621267   120.4  ns/op    0 B/op    0 allocs/op
  CmapString/16_keys    10789444   100.1  ns/op    2 B/op    1 allocs/op
  MutexMap/64_keys       9219216   127.2  ns/op    0 B/op    0 allocs/op
  CmapString/64_keys    12621823    92.49 ns/op    5 B/op    2 allocs/op

ReadHeavy (GOMAXPROCS goroutines, 90% get / 10% set):
  MutexMap/1_keys       35376198    33.17 ns/op    0 B/op    0 allocs/op
  CmapString/1_keys     19568185    53.20 ns/op    0 B/op    0 allocs/op
  MutexMap/4_keys       35922814    32.83 ns/op    0 B/op    0 allocs/op
  CmapString/4_keys     21431676    54.05 ns/op    0 B/op    0 allocs/op
  MutexMap/16_keys      37940584    31.39 ns/op    0 B/op    0 allocs/op
  CmapString/16_keys    26575219    47.20 ns/op    0 B/op    0 allocs/op
  MutexMap/64_keys      36463752    32.72 ns/op    0 B/op    0 allocs/op
  CmapString/64_keys    24454710    45.88 ns/op    1 B/op    0 allocs/op
```


Conclusion

At 1 key (the dominant case) mutex+map is 4.4x faster serial, 1.6x
faster parallel, and 1.6x faster read-heavy, with zero allocations.

At 1-4 keys mutex+map wins across every scenario. Concurrent-map only
pulls ahead in the parallel write-heavy scenario at 16+ keys (sharding
reduces lock contention), but that scenario does not match the real
access pattern: getOrCreateSession is one get-or-set, runFetch is one
delete, and most accesses are reads. Under read-heavy parallel load
mutex+map wins at all cardinalities (31-33 ns vs 45-54 ns).

Concurrent-map also requires int64-to-string conversion on every access,
costing 1-2 allocs/op at higher key counts.

Keeping sync.Mutex + map[int64] is the right choice.
