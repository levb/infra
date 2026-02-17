fetchmap benchmark: mutex+map vs concurrent-map

The review suggested replacing sync.Mutex + map[int64]*fetchSession with
a sharded concurrent map (github.com/orcaman/concurrent-map). This file
documents why we kept the mutex approach.

Argument

The fetchMap holds at most ~4 entries at a time (one per in-flight 4MB
chunk fetch). At this size, the dominant cost is synchronization overhead,
not map operations. A 32-shard concurrent map pays for shard selection,
hashing, and (for int64 keys) string conversion on every access, all for
a map that rarely has contention.

Benchmark design

Three scenarios, each comparing mutex+map against concurrent-map with
string keys (the only key type concurrent-map supports):

- Serial: single goroutine doing set/get/delete in a loop
- Parallel: GOMAXPROCS goroutines doing set/get/delete concurrently
- ReadHeavy: GOMAXPROCS goroutines, 90% gets and 10% sets

All benchmarks use 4 keys to match real-world fetchMap cardinality.
Run with -benchmem to surface allocations.

Results

```
goos: linux
goarch: amd64
cpu: AMD Ryzen 7 8845HS w/ Radeon 780M Graphics

BenchmarkFetchMap_Serial/MutexMap-16         40363016    27.53 ns/op    0 B/op    0 allocs/op
BenchmarkFetchMap_Serial/CmapString-16        9521148   122.8  ns/op    0 B/op    0 allocs/op

BenchmarkFetchMap_Parallel/MutexMap-16       11271100   106.6  ns/op    0 B/op    0 allocs/op
BenchmarkFetchMap_Parallel/CmapString-16      7153929   165.3  ns/op    0 B/op    0 allocs/op

BenchmarkFetchMap_ReadHeavy/MutexMap-16      33995158    33.79 ns/op    0 B/op    0 allocs/op
BenchmarkFetchMap_ReadHeavy/CmapString-16    22238833    50.25 ns/op    0 B/op    0 allocs/op
```

Conclusion

mutex+map is 1.5-4.5x faster than concurrent-map in every scenario.
The sharded map's overhead (hashing, shard selection, int64-to-string
conversion) is not amortized when the map only holds a handful of entries.
Keeping sync.Mutex + map[int64] is the right choice here.
