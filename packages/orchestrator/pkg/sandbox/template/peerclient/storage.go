package peerclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient")
	meter  = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient")

	peerReadTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"orchestrator.storage.peer.read",
		"Duration of peer orchestrator reads",
		"Total bytes read from peer orchestrator",
		"Total peer orchestrator reads",
	))

	attrOpFetch       = attribute.String("operation", "Fetch")
	attrOpGetBlob   = attribute.String("operation", "GetBlob")
	attrOpSize        = attribute.String("operation", "Size")

	attrResolveRedisError = attribute.String("peer_resolve", "redis_error")
	attrResolveNoPeer     = attribute.String("peer_resolve", "no_peer")
	attrResolveSelf       = attribute.String("peer_resolve", "self")
	attrResolveDialError  = attribute.String("peer_resolve", "dial_error")
	attrResolvePeer       = attribute.String("peer_resolve", "peer")
	attrResolveUploaded   = attribute.String("peer_resolve", "uploaded")

	attrPeerHitTrue  = attribute.Bool("peer_hit", true)
	attrPeerHitFalse = attribute.Bool("peer_hit", false)
)

var _ storage.Store = (*routingProvider)(nil)

// routingProvider wraps a base Store and, for each operation,
// checks Redis for a peer routing entry for the buildID extracted from the path.
type routingProvider struct {
	base     storage.Store
	resolver Resolver
}

func NewRoutingProvider(base storage.Store, resolver Resolver) storage.Store {
	return &routingProvider{base: base, resolver: resolver}
}

func (p *routingProvider) resolveProvider(ctx context.Context, buildID string) storage.Store {
	ctx, span := tracer.Start(ctx, "resolve peer-provider", trace.WithAttributes(
		telemetry.WithBuildID(buildID),
	))
	defer span.End()

	status, res := p.resolver.resolve(ctx, buildID)
	span.SetAttributes(status)

	if status != attrResolvePeer {
		return p.base
	}

	span.SetAttributes(attribute.String("peer_address", res.addr))

	return newPeerBackend(p.base, res.client, res.uploaded)
}

func (p *routingProvider) Fetch(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	buildID, _ := storage.SplitPath(path)

	return p.resolveProvider(ctx, buildID).Fetch(ctx, path, offset, length)
}

func (p *routingProvider) GetBlob(ctx context.Context, path string) ([]byte, error) {
	buildID, _ := storage.SplitPath(path)

	return p.resolveProvider(ctx, buildID).GetBlob(ctx, path)
}

func (p *routingProvider) Size(ctx context.Context, path string) (int64, error) {
	buildID, _ := storage.SplitPath(path)

	return p.resolveProvider(ctx, buildID).Size(ctx, path)
}

func (p *routingProvider) Upload(ctx context.Context, remotePath string, src io.ReaderAt, size int64) error {
	return p.base.Upload(ctx, remotePath, src, size)
}

func (p *routingProvider) PutBlob(ctx context.Context, path string, data []byte) error {
	return p.base.PutBlob(ctx, path, data)
}

func (p *routingProvider) Delete(ctx context.Context, prefix string) error {
	return p.base.Delete(ctx, prefix)
}

func (p *routingProvider) SignedUploadURL(ctx context.Context, path string, ttl time.Duration) (string, error) {
	return p.base.SignedUploadURL(ctx, path, ttl)
}

func (p *routingProvider) GetDetails() string {
	return p.base.GetDetails()
}

var _ storage.Store = (*peerBackend)(nil)

// peerBackend tries the peer first for reads. Writes are always delegated to base.
type peerBackend struct {
	base       storage.Store
	peerClient orchestrator.ChunkServiceClient
	// uploaded is set when the peer signals GCS upload is complete (use_storage=true).
	uploaded *atomic.Pointer[UploadedHeaders]
}

func newPeerBackend(
	base storage.Store,
	peerClient orchestrator.ChunkServiceClient,
	uploaded *atomic.Pointer[UploadedHeaders],
) storage.Store {
	return &peerBackend{
		base:       base,
		peerClient: peerClient,
		uploaded:   uploaded,
	}
}

func (p *peerBackend) Fetch(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	buildID, fileName := storage.SplitPath(path)

	return withPeerFallback(ctx, p, buildID, fileName, "peer-fetch", attrOpFetch,
		func(ctx context.Context) (peerAttempt[io.ReadCloser], error) {
			streamCtx, cancel := context.WithCancel(ctx)

			recv, err := openPeerSeekableStream(streamCtx, p.peerClient, &orchestrator.ReadAtBuildSeekableRequest{
				BuildId:  buildID,
				FileName: fileName,
				Offset:   offset,
				Length:   length,
			}, p.uploaded)
			if err != nil {
				cancel()

				return peerAttempt[io.ReadCloser]{}, nil
			}

			return peerAttempt[io.ReadCloser]{
				value: newPeerStreamReader(recv, cancel),
				hit:   true,
			}, nil
		},
		func(ctx context.Context) (io.ReadCloser, error) {
			// Signal the caller to swap to V4 headers if compressed headers are available.
			if p.uploaded != nil {
				if hdrs := p.uploaded.Load(); hdrs != nil && (len(hdrs.MemfileHeader) > 0 || len(hdrs.RootfsHeader) > 0) {
					return nil, &storage.PeerTransitionedError{
						MemfileHeader: hdrs.MemfileHeader,
						RootfsHeader:  hdrs.RootfsHeader,
					}
				}
			}

			return p.base.Fetch(ctx, path, offset, length)
		},
	)
}

func (p *peerBackend) GetBlob(ctx context.Context, path string) ([]byte, error) {
	buildID, fileName := storage.SplitPath(path)

	return withPeerFallback(ctx, p, buildID, fileName, "peer-get-object", attrOpGetBlob,
		func(ctx context.Context) (peerAttempt[[]byte], error) {
			recv, err := openPeerBlobStream(ctx, p.peerClient, &orchestrator.GetBuildBlobRequest{
				BuildId:  buildID,
				FileName: fileName,
			}, p.uploaded)
			if err != nil {
				return peerAttempt[[]byte]{}, nil
			}

			var buf bytes.Buffer
			n, err := io.Copy(&buf, newPeerStreamReader(recv, func() {}))
			if err != nil {
				return peerAttempt[[]byte]{value: buf.Bytes(), bytes: n, hit: true},
					fmt.Errorf("failed to stream file %q from peer: %w", fileName, err)
			}

			return peerAttempt[[]byte]{value: buf.Bytes(), bytes: n, hit: true}, nil
		},
		func(ctx context.Context) ([]byte, error) {
			return p.base.GetBlob(ctx, path)
		},
	)
}

func (p *peerBackend) Size(ctx context.Context, path string) (int64, error) {
	buildID, fileName := storage.SplitPath(path)

	return withPeerFallback(ctx, p, buildID, fileName, "peer-size", attrOpSize,
		func(ctx context.Context) (peerAttempt[int64], error) {
			resp, err := p.peerClient.GetBuildFileSize(ctx, &orchestrator.GetBuildFileSizeRequest{
				BuildId:  buildID,
				FileName: fileName,
			})
			if err == nil && checkPeerAvailability(resp.GetAvailability(), p.uploaded) {
				return peerAttempt[int64]{value: resp.GetTotalSize(), hit: true}, nil
			}

			return peerAttempt[int64]{}, nil
		},
		func(ctx context.Context) (int64, error) {
			return p.base.Size(ctx, path)
		},
	)
}

func (p *peerBackend) Upload(ctx context.Context, remotePath string, src io.ReaderAt, size int64) error {
	return p.base.Upload(ctx, remotePath, src, size)
}

func (p *peerBackend) PutBlob(ctx context.Context, path string, data []byte) error {
	return p.base.PutBlob(ctx, path, data)
}

func (p *peerBackend) Delete(ctx context.Context, prefix string) error {
	return p.base.Delete(ctx, prefix)
}

func (p *peerBackend) SignedUploadURL(ctx context.Context, path string, ttl time.Duration) (string, error) {
	return p.base.SignedUploadURL(ctx, path, ttl)
}

func (p *peerBackend) GetDetails() string {
	return p.base.GetDetails()
}

// checkPeerAvailability marks the build as uploaded when UseStorage is set.
func checkPeerAvailability(avail *orchestrator.PeerAvailability, uploaded *atomic.Pointer[UploadedHeaders]) bool {
	if avail.GetNotAvailable() {
		return false
	}

	if avail.GetUseStorage() {
		hdrs := &UploadedHeaders{
			MemfileHeader: avail.GetMemfileHeader(),
			RootfsHeader:  avail.GetRootfsHeader(),
		}
		uploaded.Store(hdrs)

		return false
	}

	return true
}

// peerAttempt is the result of a peer read attempt, used with withPeerFallback.
type peerAttempt[T any] struct {
	value T
	bytes int64
	hit   bool
}

func withPeerFallback[T any](
	ctx context.Context,
	p *peerBackend,
	buildID, fileName string,
	spanName string,
	opAttr attribute.KeyValue,
	peerFn func(ctx context.Context) (peerAttempt[T], error),
	useBase func(ctx context.Context) (T, error),
) (T, error) {
	ctx, span := tracer.Start(ctx, spanName, trace.WithAttributes(
		attribute.String("file_name", fileName),
	))
	defer span.End()

	if p.uploaded.Load() == nil {
		timer := peerReadTimerFactory.Begin(opAttr)

		res, err := peerFn(ctx)
		if res.hit {
			if err != nil {
				span.RecordError(err)
				timer.Failure(ctx, res.bytes)

				return res.value, err
			}

			span.SetAttributes(attrPeerHitTrue)
			timer.Success(ctx, res.bytes)

			return res.value, nil
		}

		if err != nil {
			span.RecordError(err)
		}

		timer.Failure(ctx, 0)
	}

	span.SetAttributes(attrPeerHitFalse)

	result, err := useBase(ctx)
	if err != nil {
		span.RecordError(err)
	}

	return result, err
}

var _ io.ReadCloser = (*peerStreamReader)(nil)

// peerStreamReader wraps a gRPC streaming recv function as an io.ReadCloser.
type peerStreamReader struct {
	recv    func() ([]byte, error)
	current *bytes.Reader
	done    bool
	cancel  context.CancelFunc
}

func newPeerStreamReader(recv func() ([]byte, error), cancel context.CancelFunc) *peerStreamReader {
	return &peerStreamReader{
		recv:   recv,
		cancel: cancel,
	}
}

func (r *peerStreamReader) Read(p []byte) (int, error) {
	for {
		if r.current != nil && r.current.Len() > 0 {
			return r.current.Read(p)
		}

		if r.done {
			return 0, io.EOF
		}

		data, err := r.recv()
		if errors.Is(err, io.EOF) {
			r.done = true

			return 0, io.EOF
		}
		if err != nil {
			return 0, fmt.Errorf("failed to receive chunk from peer: %w", err)
		}

		r.current = bytes.NewReader(data)
	}
}

func (r *peerStreamReader) Close() error {
	r.cancel()

	return nil
}

// openPeerSeekableStream opens a ReadAtBuildSeekable stream, checks peer availability,
// and returns a recv function that yields data chunks starting with the first message's data.
func openPeerSeekableStream(
	ctx context.Context,
	client orchestrator.ChunkServiceClient,
	req *orchestrator.ReadAtBuildSeekableRequest,
	uploaded *atomic.Pointer[UploadedHeaders],
) (func() ([]byte, error), error) {
	stream, err := client.ReadAtBuildSeekable(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("open seekable stream: %w", err)
	}

	msg, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("recv first seekable message: %w", err)
	}

	if !checkPeerAvailability(msg.GetAvailability(), uploaded) {
		return nil, fmt.Errorf("peer not available for seekable stream")
	}

	first := msg.GetData()

	return func() ([]byte, error) {
		if first != nil {
			data := first
			first = nil

			return data, nil
		}

		m, err := stream.Recv()
		if err != nil {
			return nil, err
		}

		checkPeerAvailability(m.GetAvailability(), uploaded)

		return m.GetData(), nil
	}, nil
}

// openPeerBlobStream opens a GetBuildBlob stream, checks peer availability,
// and returns a recv function that yields data chunks.
func openPeerBlobStream(
	ctx context.Context,
	client orchestrator.ChunkServiceClient,
	req *orchestrator.GetBuildBlobRequest,
	uploaded *atomic.Pointer[UploadedHeaders],
) (func() ([]byte, error), error) {
	stream, err := client.GetBuildBlob(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("open blob stream: %w", err)
	}

	msg, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("recv first blob message: %w", err)
	}

	if !checkPeerAvailability(msg.GetAvailability(), uploaded) {
		return nil, fmt.Errorf("peer not available for blob stream")
	}

	first := msg.GetData()

	return func() ([]byte, error) {
		if first != nil {
			data := first
			first = nil

			return data, nil
		}

		m, err := stream.Recv()
		if err != nil {
			return nil, err
		}

		checkPeerAvailability(m.GetAvailability(), uploaded)

		return m.GetData(), nil
	}, nil
}
