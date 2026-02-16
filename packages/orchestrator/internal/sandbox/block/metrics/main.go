package metrics

import (
	"fmt"

	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	orchestratorBlockSlices          = "orchestrator.blocks.slices"
	orchestratorBlockChunksFetch     = "orchestrator.blocks.chunks.fetch"
	orchestratorBlockChunksStore     = "orchestrator.blocks.chunks.store"
	orchestratorBlockChunkerCreation = "orchestrator.blocks.chunker.creation"
)

type Metrics struct {
	// SlicesTimerFactory measures page faulting performance.
	SlicesTimerFactory telemetry.TimerFactory

	// RemoteReadsTimerFactory measures the time taken to download chunks from remote storage.
	RemoteReadsTimerFactory telemetry.TimerFactory

	// WriteChunksTimerFactory measures performance of writing chunks to disk.
	WriteChunksTimerFactory telemetry.TimerFactory

	// ChunkerCreations counts chunker instantiations by type and compression mode.
	ChunkerCreations metric.Int64Counter
}

func NewMetrics(meterProvider metric.MeterProvider) (Metrics, error) {
	var m Metrics

	blocksMeter := meterProvider.Meter("internal.sandbox.block.metrics")

	var err error
	if m.SlicesTimerFactory, err = telemetry.NewTimerFactory(
		blocksMeter, orchestratorBlockSlices,
		"Time taken to retrieve memory slices",
		"Total bytes requested",
		"Total page faults",
	); err != nil {
		return m, fmt.Errorf("error creating slices timer factory: %w", err)
	}

	if m.RemoteReadsTimerFactory, err = telemetry.NewTimerFactory(
		blocksMeter, orchestratorBlockChunksFetch,
		"Time taken to fetch memory chunks from remote store",
		"Total bytes fetched from remote store",
		"Total remote fetches",
	); err != nil {
		return m, fmt.Errorf("error creating reads timer factory: %w", err)
	}

	if m.WriteChunksTimerFactory, err = telemetry.NewTimerFactory(
		blocksMeter, orchestratorBlockChunksStore,
		"Time taken to write memory chunks to disk",
		"Total bytes written to disk",
		"Total cache writes",
	); err != nil {
		return m, fmt.Errorf("failed to get stored chunks metric: %w", err)
	}

	if m.ChunkerCreations, err = blocksMeter.Int64Counter(
		orchestratorBlockChunkerCreation,
		metric.WithDescription("Number of chunker instantiations"),
	); err != nil {
		return m, fmt.Errorf("failed to create chunker creation counter: %w", err)
	}

	return m, nil
}
