package storage

import (
	"context"
	"io"
	"time"

	"github.com/stretchr/testify/mock"
)

// MockStore is a testify mock for the Store interface.
type MockStore struct {
	mock.Mock
}

func NewMockStore(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockStore {
	m := &MockStore{}
	m.Mock.Test(t)
	t.Cleanup(func() { m.AssertExpectations(t) })

	return m
}

func (m *MockStore) Fetch(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	args := m.Called(ctx, path, offset, length)
	var rc io.ReadCloser
	if v := args.Get(0); v != nil {
		rc = v.(io.ReadCloser)
	}

	return rc, args.Error(1)
}

func (m *MockStore) GetBlob(ctx context.Context, path string) ([]byte, error) {
	args := m.Called(ctx, path)
	var data []byte
	if v := args.Get(0); v != nil {
		data = v.([]byte)
	}

	return data, args.Error(1)
}

func (m *MockStore) Size(ctx context.Context, path string) (int64, error) {
	args := m.Called(ctx, path)

	return args.Get(0).(int64), args.Error(1)
}

func (m *MockStore) Upload(ctx context.Context, remotePath string, src io.ReaderAt, size int64) error {
	args := m.Called(ctx, remotePath, src, size)

	return args.Error(0)
}

func (m *MockStore) PutBlob(ctx context.Context, path string, data []byte) error {
	args := m.Called(ctx, path, data)

	return args.Error(0)
}

func (m *MockStore) Delete(ctx context.Context, prefix string) error {
	args := m.Called(ctx, prefix)

	return args.Error(0)
}

func (m *MockStore) SignedUploadURL(ctx context.Context, path string, ttl time.Duration) (string, error) {
	args := m.Called(ctx, path, ttl)

	return args.String(0), args.Error(1)
}

func (m *MockStore) GetDetails() string {
	args := m.Called()

	return args.String(0)
}
