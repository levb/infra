package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helper to create a FileSystemStorageProvider rooted in a temp directory.
func newTempProvider(t *testing.T) *fsStore {
	t.Helper()

	base := t.TempDir()
	p := newFileSystemStore(StoreConfig{
		GetLocalBasePath: func() string { return base },
	})

	return p
}

func TestOpenObject_Write_Size_GetBlob(t *testing.T) {
	t.Parallel()
	p := newTempProvider(t)
	ctx := t.Context()

	path := filepath.Join("sub", "file.txt")
	contents := []byte("hello world")
	// write via PutBlob
	err := p.PutBlob(ctx, path, contents)
	require.NoError(t, err)

	// check Size
	size, err := p.Size(ctx, path)
	require.NoError(t, err)
	require.Equal(t, int64(len(contents)), size)

	// read back via GetBlob
	data, err := p.GetBlob(ctx, path)
	require.NoError(t, err)
	require.Equal(t, contents, data)
}

func TestFSPut(t *testing.T) {
	t.Parallel()
	p := newTempProvider(t)
	ctx := t.Context()

	const payload = "copy me please"
	err := p.PutBlob(ctx, "copy/dst.txt", []byte(payload))
	require.NoError(t, err)

	data, err := p.GetBlob(ctx, "copy/dst.txt")
	require.NoError(t, err)
	require.Equal(t, payload, string(data))
}

func TestDelete(t *testing.T) {
	t.Parallel()
	p := newTempProvider(t)
	ctx := t.Context()

	err := p.PutBlob(ctx, "to/delete.txt", []byte("bye"))
	require.NoError(t, err)

	_, err = p.Size(ctx, "to/delete.txt")
	require.NoError(t, err)

	err = p.Delete(t.Context(), "to/delete.txt")
	require.NoError(t, err)

	// subsequent Size call should fail with ErrorObjectNotExist
	_, err = p.Size(ctx, "to/delete.txt")
	require.ErrorIs(t, err, ErrObjectNotExist)
}

func TestDeleteObjectsWithPrefix(t *testing.T) {
	t.Parallel()
	p := newTempProvider(t)
	ctx := t.Context()

	paths := []string{
		"data/a.txt",
		"data/b.txt",
		"data/sub/c.txt",
	}
	for _, pth := range paths {
		err := p.PutBlob(ctx, pth, []byte("x"))
		require.NoError(t, err)
	}

	// remove the entire "data" prefix
	require.NoError(t, p.Delete(ctx, "data"))

	for _, pth := range paths {
		full := filepath.Join(p.basePath, pth)
		_, err := os.Stat(full)
		require.True(t, os.IsNotExist(err))
	}
}

func TestGetBlobNonExistent(t *testing.T) {
	t.Parallel()
	p := newTempProvider(t)

	_, err := p.GetBlob(t.Context(), "missing/file.txt")
	require.ErrorIs(t, err, ErrObjectNotExist)
}

func TestFSFetch(t *testing.T) {
	t.Parallel()
	p := newTempProvider(t)
	ctx := t.Context()

	data := []byte("hello world, this is a test")
	err := p.PutBlob(ctx, "fetch/test.bin", data)
	require.NoError(t, err)

	// Fetch a range
	rc, err := p.Fetch(ctx, "fetch/test.bin", 6, 5)
	require.NoError(t, err)
	defer rc.Close()

	buf := make([]byte, 5)
	n, err := rc.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "world", string(buf))
}
