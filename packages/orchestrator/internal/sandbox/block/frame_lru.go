package block

import (
	lru "github.com/hashicorp/golang-lru"
)

// DefaultLRUFrameCount is the default number of decompressed frames to keep in the LRU cache.
const DefaultLRUFrameCount = 16

type cachedFrame struct {
	data   []byte
	offset int64
	size   int64
}

// FrameLRU provides an in-memory LRU cache for decompressed frames.
type FrameLRU struct {
	cache *lru.Cache
}

func NewFrameLRU(maxFrames int) (*FrameLRU, error) {
	cache, err := lru.New(maxFrames)
	if err != nil {
		return nil, err
	}

	return &FrameLRU{
		cache: cache,
	}, nil
}

func (l *FrameLRU) get(frameOffset int64) (*cachedFrame, bool) {
	val, ok := l.cache.Get(frameOffset)
	if !ok {
		return nil, false
	}

	frame, ok := val.(*cachedFrame)
	if !ok {
		return nil, false
	}

	return frame, true
}

func (l *FrameLRU) put(frameOffset int64, frameSize int64, data []byte) {
	frame := &cachedFrame{
		data:   data,
		offset: frameOffset,
		size:   frameSize,
	}
	l.cache.Add(frameOffset, frame)
}

func (l *FrameLRU) Purge() {
	l.cache.Purge()
}
