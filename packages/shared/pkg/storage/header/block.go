package header

func TotalBlocks(size, blockSize int) int {
	return (size + blockSize - 1) / blockSize
}

func BlocksOffsets(size, blockSize int) []int {
	offsets := make([]int, TotalBlocks(size, blockSize))

	for i := range offsets {
		offsets[i] = BlockOffset(i, blockSize)
	}

	return offsets
}

func BlockIdx(off, blockSize int) int {
	return off / blockSize
}

func BlockOffset(idx, blockSize int) int {
	return idx * blockSize
}
