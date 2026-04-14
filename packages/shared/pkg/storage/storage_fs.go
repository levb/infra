package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type fsStore struct {
	basePath  string
	uploadURL string // base URL for local upload endpoint (e.g. "http://localhost:5008")
	hmacKey   []byte // HMAC key for signing upload tokens
}

var _ Store = (*fsStore)(nil)

type fsRangeReadCloser struct {
	io.Reader

	file *os.File
}

func (r *fsRangeReadCloser) Close() error {
	return r.file.Close()
}

func newFileSystemStore(cfg StoreConfig) *fsStore {
	return &fsStore{
		basePath:  cfg.GetLocalBasePath(),
		uploadURL: cfg.uploadBaseURL,
		hmacKey:   cfg.hmacKey,
	}
}

func (s *fsStore) Fetch(_ context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	fullPath := s.getPath(path)

	f, err := s.openExisting(fullPath)
	if err != nil {
		return nil, err
	}

	return &fsRangeReadCloser{
		Reader: io.NewSectionReader(f, offset, length),
		file:   f,
	}, nil
}

func (s *fsStore) GetBlob(_ context.Context, path string) ([]byte, error) {
	fullPath := s.getPath(path)

	f, err := s.openExisting(fullPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, f); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (s *fsStore) Size(_ context.Context, path string) (int64, error) {
	fullPath := s.getPath(path)

	f, err := s.openExisting(fullPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	fileInfo, err := f.Stat()
	if err != nil {
		return 0, err
	}

	// Check for .uncompressed-size sidecar file
	sidecarPath := fullPath + "." + MetadataKeyUncompressedSize
	if sidecarData, sidecarErr := os.ReadFile(sidecarPath); sidecarErr == nil {
		if parsed, parseErr := strconv.ParseInt(strings.TrimSpace(string(sidecarData)), 10, 64); parseErr == nil {
			return parsed, nil
		}
	}

	return fileInfo.Size(), nil
}

func (s *fsStore) PutBlob(_ context.Context, path string, data []byte) error {
	fullPath := s.getPath(path)

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	f, err := os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, bytes.NewReader(data))

	return err
}

func (s *fsStore) Upload(_ context.Context, remotePath string, src io.ReaderAt, size int64) error {
	fullPath := s.getPath(remotePath)

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	f, err := os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, io.NewSectionReader(src, 0, size))

	return err
}

func (s *fsStore) Delete(_ context.Context, prefix string) error {
	filePath := s.getPath(prefix)

	return os.RemoveAll(filePath)
}

func (s *fsStore) SignedUploadURL(_ context.Context, path string, ttl time.Duration) (string, error) {
	if s.uploadURL == "" || s.hmacKey == nil {
		return "", fmt.Errorf("file system storage does not support signed URLs (no local upload endpoint configured)")
	}

	expires := time.Now().Add(ttl).Unix()
	token := ComputeUploadHMAC(s.hmacKey, path, expires)

	u := fmt.Sprintf("%s/upload?path=%s&expires=%d&token=%s",
		s.uploadURL, url.QueryEscape(path), expires, url.QueryEscape(token))

	return u, nil
}

func (s *fsStore) GetDetails() string {
	return fmt.Sprintf("[Local file storage, base path set to %s]", s.basePath)
}

func (s *fsStore) getPath(path string) string {
	return filepath.Join(s.basePath, path)
}

func (s *fsStore) openExisting(fullPath string) (*os.File, error) {
	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrObjectNotExist
		}

		return nil, err
	}

	if info.IsDir() {
		return nil, fmt.Errorf("path %s is a directory", fullPath)
	}

	return os.Open(fullPath)
}

func (s *fsStore) NewPartUploader(_ context.Context, remotePath string, metadata map[string]string) (PartUploader, error) {
	fullPath := s.getPath(remotePath)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return nil, err
	}

	// Write sidecar files for metadata that FS needs on disk (e.g. uncompressed size for Size()).
	if v, ok := metadata[MetadataKeyUncompressedSize]; ok {
		sidecarPath := fullPath + "." + MetadataKeyUncompressedSize
		if err := os.WriteFile(sidecarPath, []byte(v), 0o644); err != nil {
			return nil, fmt.Errorf("failed to write sidecar %s: %w", sidecarPath, err)
		}
	}

	return &fsPartUploader{fullPath: fullPath}, nil
}

func ComputeUploadHMAC(key []byte, path string, expires int64) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(path))
	mac.Write([]byte{0}) // delimiter to prevent path/expires boundary ambiguity
	mac.Write([]byte(strconv.FormatInt(expires, 10)))

	return hex.EncodeToString(mac.Sum(nil))
}

// ValidateUploadToken validates an HMAC token for a local upload URL.
// Exported so that the upload handler in the orchestrator can use it.
func ValidateUploadToken(key []byte, path string, expires int64, token string) bool {
	if time.Now().Unix() > expires {
		return false
	}

	expected := ComputeUploadHMAC(key, path, expires)

	return hmac.Equal([]byte(expected), []byte(token))
}

// fsPartUploader implements partUploader for local filesystem.
// Embeds memPartUploader for concurrent-safe part collection,
// then writes atomically on Complete.
type fsPartUploader struct {
	memPartUploader

	fullPath string
}

func (u *fsPartUploader) Complete(_ context.Context) error {
	if err := os.MkdirAll(filepath.Dir(u.fullPath), 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	return os.WriteFile(u.fullPath, u.Assemble(), 0o644)
}
