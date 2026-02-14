package storage

const (
	GuestEnvdPath = "/usr/bin/envd"

	MemfileName  = "memfile"
	RootfsName   = "rootfs.ext4"
	SnapfileName = "snapfile"
	MetadataName = "metadata.json"

	HeaderSuffix = ".header"

	// CompressedHeaderSuffix is the object-path suffix for compressed headers.
	// Hardcoded to LZ4 — always the fastest to decompress, independent of
	// the data compression algorithm.
	CompressedHeaderSuffix = ".compressed.header.lz4"
)

type TemplateFiles struct {
	BuildID string `json:"build_id"`
}

// Key for the cache. Unique for template-build pair.
func (t TemplateFiles) CacheKey() string {
	return t.BuildID
}

func (t TemplateFiles) StorageDir() string {
	return t.BuildID
}

// Path returns "{buildId}/{fileName}".
func (t TemplateFiles) Path(fileName string) string {
	return t.BuildID + "/" + fileName
}

// HeaderPath returns "{buildId}/{fileName}.header".
func (t TemplateFiles) HeaderPath(fileName string) string {
	return t.BuildID + "/" + fileName + HeaderSuffix
}

// CompressedPath returns "{buildId}/{fileName}.{defaultCompressionSuffix}".
// Write-side only — the read side derives the suffix from the frame table.
func (t TemplateFiles) CompressedPath(fileName string) string {
	return t.BuildID + "/" + fileName + DefaultCompressionOptions.CompressionType.Suffix()
}

// CompressedHeaderPath returns "{buildId}/{fileName}.compressed.header.lz4".
func (t TemplateFiles) CompressedHeaderPath(fileName string) string {
	return t.BuildID + "/" + fileName + CompressedHeaderSuffix
}
