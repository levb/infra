package storage

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Precomputed attribute sets for hot-path read.*/chunk.* emissions; cold error
// paths build attrs inline via ErrAttrs.

// Source identifies the backend that served a read. The zero value (SourceMmap)
// is also the default for pre-resolution failures.
type Source int8

const (
	// Order is latency-ascending and load-bearing: a multi-chunk Slice records
	// the slowest source it touched via max() over per-fetch sources.
	SourceMmap Source = iota
	SourceFS
	SourcePeer
	SourceNFS
	SourceGCS
	SourceAWS
	numSources
)

func (s Source) String() string { return sourceStrings[s] }

var sourceStrings = [numSources]string{
	SourceMmap: "mmap",
	SourceFS:   "fs",
	SourcePeer: "peer",
	SourceNFS:  "nfs",
	SourceGCS:  "gcs",
	SourceAWS:  "aws",
}

const numCodecs = 3 // CompressionNone, Zstd, LZ4

var (
	tableOK [numSeekableObjectTypes][numSources][numCodecs]metric.MeasurementOption

	tableCacheHit          [numSeekableObjectTypes][numSources][numCodecs]metric.MeasurementOption
	tableCacheMiss         [numSeekableObjectTypes][numSources][numCodecs]metric.MeasurementOption
	tableCacheWritebackOK  [numSeekableObjectTypes][numSources][numCodecs]metric.MeasurementOption
	tableCacheWritebackErr [numSeekableObjectTypes][numSources][numCodecs]metric.MeasurementOption

	// keyed by file_type only: inflight is incremented before the source is
	// known (the OpenRangeReader call itself dominates GCS latency).
	tableInflightFetch [numSeekableObjectTypes]metric.MeasurementOption
)

func init() {
	set := func(kvs ...attribute.KeyValue) metric.MeasurementOption {
		return metric.WithAttributeSet(attribute.NewSet(kvs...))
	}

	for ot := range numSeekableObjectTypes {
		ftAttr := attribute.String(AttrFileType, ot.String())

		tableInflightFetch[ot] = set(ftAttr)

		for s := range numSources {
			srcAttr := attribute.String(AttrSource, sourceStrings[s])

			for ct := range CompressionType(numCodecs) {
				codecAttr := attribute.String(AttrCodec, ct.String())
				outcomeOK := attribute.String(AttrOutcome, OutcomeOK)

				tableOK[ot][s][ct] = set(
					ftAttr, srcAttr, codecAttr, outcomeOK,
				)

				tableCacheHit[ot][s][ct] = set(
					ftAttr, attribute.String(AttrEvent, CacheEventHit),
					srcAttr, codecAttr,
				)
				tableCacheMiss[ot][s][ct] = set(
					ftAttr, attribute.String(AttrEvent, CacheEventMiss),
					srcAttr, codecAttr,
				)
				tableCacheWritebackOK[ot][s][ct] = set(
					ftAttr, attribute.String(AttrEvent, CacheEventWritebackOK),
					srcAttr, codecAttr,
				)
				tableCacheWritebackErr[ot][s][ct] = set(
					ftAttr, attribute.String(AttrEvent, CacheEventWritebackErr),
					srcAttr, codecAttr,
				)
			}
		}
	}
}

func OKAttrs(o SeekableObjectType, s Source, c CompressionType) metric.MeasurementOption {
	return tableOK[o][s][c]
}

func CacheHitAttrs(o SeekableObjectType, s Source, c CompressionType) metric.MeasurementOption {
	return tableCacheHit[o][s][c]
}

func CacheMissAttrs(o SeekableObjectType, s Source, c CompressionType) metric.MeasurementOption {
	return tableCacheMiss[o][s][c]
}

func CacheWritebackOKAttrs(o SeekableObjectType, s Source, c CompressionType) metric.MeasurementOption {
	return tableCacheWritebackOK[o][s][c]
}

func CacheWritebackErrAttrs(o SeekableObjectType, s Source, c CompressionType) metric.MeasurementOption {
	return tableCacheWritebackErr[o][s][c]
}

func InflightFetchAttrs(o SeekableObjectType) metric.MeasurementOption {
	return tableInflightFetch[o]
}
