package storage

import (
	"context"
	"fmt"
)

// Compression types and frame-based access for compressed assets.

type CompressionType byte

const (
	CompressionNone = CompressionType(iota)
	CompressionZstd
	CompressionLZ4
)

func (ct CompressionType) Suffix() string {
	switch ct {
	case CompressionZstd:
		return ".zst"
	case CompressionLZ4:
		return ".lz4"
	default:
		return ""
	}
}

func (ct CompressionType) String() string {
	switch ct {
	case CompressionZstd:
		return "zstd"
	case CompressionLZ4:
		return "lz4"
	default:
		return "none"
	}
}

type FrameOffset struct {
	U int64
	C int64
}

func (o *FrameOffset) String() string {
	return fmt.Sprintf("U:%#x/C:%#x", o.U, o.C)
}

func (o *FrameOffset) Add(f FrameSize) {
	o.U += int64(f.U)
	o.C += int64(f.C)
}

type FrameSize struct {
	U int32
	C int32
}

func (s FrameSize) String() string {
	return fmt.Sprintf("U:%#x/C:%#x", s.U, s.C)
}

type Range struct {
	Start  int64
	Length int
}

func (r Range) String() string {
	return fmt.Sprintf("%#x/%#x", r.Start, r.Length)
}

type FrameTable struct {
	CompressionType CompressionType
	StartAt         FrameOffset
	Frames          []FrameSize
}

// CompressionTypeSuffix returns the object-path suffix for this frame table's
// compression type. Returns "" when ft is nil.
func (ft *FrameTable) CompressionTypeSuffix() string {
	if ft == nil {
		return ""
	}

	return ft.CompressionType.Suffix()
}

// FrameGetter reads a single compressed or uncompressed frame from storage.
type FrameGetter interface {
	GetFrame(ctx context.Context, objectPath string, offsetU int64, frameTable *FrameTable, decompress bool, buf []byte) (Range, error)
}

// IsCompressed returns true if the frame table represents compressed data.
// Safe to call with nil - returns false.
func IsCompressed(ft *FrameTable) bool {
	return ft != nil && ft.CompressionType != CompressionNone
}

// Range iterates over frames that overlap with the given range and calls fn for each frame.
func (ft *FrameTable) Range(start, length int64, fn func(offset FrameOffset, frame FrameSize) error) error {
	var currentOffset FrameOffset
	for _, frame := range ft.Frames {
		frameEnd := currentOffset.U + int64(frame.U)
		requestEnd := start + length
		if frameEnd <= start {
			// frame is before the requested range
			currentOffset.U += int64(frame.U)
			currentOffset.C += int64(frame.C)

			continue
		}
		if currentOffset.U >= requestEnd {
			// frame is after the requested range
			break
		}

		// frame overlaps with the requested range
		if err := fn(currentOffset, frame); err != nil {
			return err
		}
		currentOffset.U += int64(frame.U)
		currentOffset.C += int64(frame.C)
	}

	return nil
}

func (ft *FrameTable) Size() (U, C int64) {
	for _, frame := range ft.Frames {
		U += int64(frame.U)
		C += int64(frame.C)
	}

	return U, C
}

// Subset returns a new FrameTable that represents the minimal set of frames
// that cover the start(length) range. Only entire frames are included (since
// they are compressed and can not be sliced). All offsets and sizes are in
// memory/uncompressed bytes. If the requested range extends beyond the total
// uncompressed size, the subset silently stops at the end of the frameset.
func (ft *FrameTable) Subset(r Range) (*FrameTable, error) {
	if ft == nil || r.Length == 0 {
		return nil, nil
	}
	if r.Start < ft.StartAt.U {
		return nil, fmt.Errorf("requested range starts before the beginning of the frame table")
	}
	newFrameTable := &FrameTable{
		CompressionType: ft.CompressionType,
	}

	startSet := false
	currentOffset := ft.StartAt
	requestedEnd := r.Start + int64(r.Length)
	for _, frame := range ft.Frames {
		frameEnd := currentOffset.U + int64(frame.U)
		if frameEnd <= r.Start {
			currentOffset.Add(frame)

			continue
		}
		if currentOffset.U >= requestedEnd {
			break
		}

		if !startSet {
			newFrameTable.StartAt = currentOffset
			startSet = true
		}
		newFrameTable.Frames = append(newFrameTable.Frames, frame)
		currentOffset.Add(frame)
	}

	if !startSet {
		return nil, fmt.Errorf("requested range is beyond the end of the frame table")
	}

	return newFrameTable, nil
}

// FrameFor finds the frame containing the given offset and returns its start position and full size.
func (ft *FrameTable) FrameFor(offset int64) (starts FrameOffset, size FrameSize, err error) {
	if ft == nil {
		return FrameOffset{}, FrameSize{}, fmt.Errorf("FrameFor called with nil frame table - data is not compressed")
	}

	currentOffset := ft.StartAt
	for _, frame := range ft.Frames {
		frameEnd := currentOffset.U + int64(frame.U)
		if offset >= currentOffset.U && offset < frameEnd {
			return currentOffset, frame, nil
		}
		currentOffset.Add(frame)
	}

	return FrameOffset{}, FrameSize{}, fmt.Errorf("offset %#x is beyond the end of the frame table", offset)
}

// GetFetchRange translates an uncompressed range to a compressed range using the frame table.
func (ft *FrameTable) GetFetchRange(rangeU Range) (Range, error) {
	fetchRange := rangeU
	if ft != nil && ft.CompressionType != CompressionNone {
		start, size, err := ft.FrameFor(rangeU.Start)
		if err != nil {
			return Range{}, fmt.Errorf("getting frame for offset %#x: %w", rangeU.Start, err)
		}
		endOffset := rangeU.Start + int64(rangeU.Length)
		frameEnd := start.U + int64(size.U)
		if endOffset > frameEnd {
			return Range{}, fmt.Errorf("range %v spans beyond frame ending at %#x", rangeU, frameEnd)
		}
		fetchRange = Range{
			Start:  start.C,
			Length: int(size.C),
		}
	}

	return fetchRange, nil
}
