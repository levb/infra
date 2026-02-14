package storage

import (
	"fmt"
)

const (
	GuestEnvdPath = "/usr/bin/envd"

	MemfileName  = "memfile"
	RootfsName   = "rootfs.ext4"
	SnapfileName = "snapfile"
	MetadataName = "metadata.json"

	HeaderSuffix = ".header"
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

func (t TemplateFiles) StorageMemfilePath() string {
	return fmt.Sprintf("%s/%s", t.StorageDir(), MemfileName)
}

func (t TemplateFiles) StorageMemfileHeaderPath() string {
	return fmt.Sprintf("%s/%s%s", t.StorageDir(), MemfileName, HeaderSuffix)
}

func (t TemplateFiles) StorageRootfsPath() string {
	return fmt.Sprintf("%s/%s", t.StorageDir(), RootfsName)
}

func (t TemplateFiles) StorageRootfsHeaderPath() string {
	return fmt.Sprintf("%s/%s%s", t.StorageDir(), RootfsName, HeaderSuffix)
}

func (t TemplateFiles) StorageSnapfilePath() string {
	return fmt.Sprintf("%s/%s", t.StorageDir(), SnapfileName)
}

func (t TemplateFiles) StorageMetadataPath() string {
	return fmt.Sprintf("%s/%s", t.StorageDir(), MetadataName)
}

// Path returns the storage path for a given file name within this build.
func (t TemplateFiles) Path(fileName string) string {
	return fmt.Sprintf("%s/%s", t.StorageDir(), fileName)
}

// HeaderPath returns the header storage path for a given file name within this build.
func (t TemplateFiles) HeaderPath(fileName string) string {
	return fmt.Sprintf("%s/%s%s", t.StorageDir(), fileName, HeaderSuffix)
}

const CompressedHeaderSuffix = ".compressed.header.lz4"

// CompressedPath returns the compressed data path for a given file name.
func (t TemplateFiles) CompressedPath(fileName string) string {
	return fmt.Sprintf("%s/%s%s", t.StorageDir(), fileName, DefaultCompressionSuffix)
}

// CompressedHeaderPath returns the compressed header path for a given file name.
func (t TemplateFiles) CompressedHeaderPath(fileName string) string {
	return fmt.Sprintf("%s/%s%s", t.StorageDir(), fileName, CompressedHeaderSuffix)
}

// DefaultCompressionSuffix is the file extension for compressed assets.
var DefaultCompressionSuffix = CompressionLZ4.Suffix()
