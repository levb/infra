package cmdutil

import (
	"fmt"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

const NilUUID = "00000000-0000-0000-0000-000000000000"

// FormatMappingWithCompression returns mapping info with compression details.
func FormatMappingWithCompression(mapping *header.BuildMap, blockSize uint64) string {
	base := mapping.Format(blockSize)

	if mapping.FrameTable == nil {
		return base + " [uncompressed]"
	}

	ft := mapping.FrameTable
	var totalU, totalC int64
	for _, frame := range ft.Frames {
		totalU += int64(frame.U)
		totalC += int64(frame.C)
	}

	ratio := float64(totalU) / float64(totalC)

	return fmt.Sprintf("%s [%s: %d frames, U=%d C=%d ratio=%.2fx]",
		base, ft.CompressionType.String(), len(ft.Frames), totalU, totalC, ratio)
}

// PrintCompressionSummary prints compression statistics for a header.
func PrintCompressionSummary(h *header.Header) {
	var compressedMappings, uncompressedMappings int
	var totalUncompressedBytes, totalCompressedBytes int64
	var totalFrames int

	type buildStats struct {
		uncompressedBytes int64
		compressedBytes   int64
		frames            int
		compressed        bool
	}
	buildCompressionStats := make(map[string]*buildStats)

	for _, mapping := range h.Mapping {
		buildID := mapping.BuildId.String()
		if buildID == NilUUID {
			continue
		}

		if _, ok := buildCompressionStats[buildID]; !ok {
			buildCompressionStats[buildID] = &buildStats{}
		}
		stats := buildCompressionStats[buildID]

		if mapping.FrameTable != nil && mapping.FrameTable.CompressionType != storage.CompressionNone {
			compressedMappings++
			stats.compressed = true

			for _, frame := range mapping.FrameTable.Frames {
				totalUncompressedBytes += int64(frame.U)
				totalCompressedBytes += int64(frame.C)
				stats.uncompressedBytes += int64(frame.U)
				stats.compressedBytes += int64(frame.C)
			}
			totalFrames += len(mapping.FrameTable.Frames)
			stats.frames += len(mapping.FrameTable.Frames)
		} else {
			uncompressedMappings++
			totalUncompressedBytes += int64(mapping.Length)
			stats.uncompressedBytes += int64(mapping.Length)
		}
	}

	fmt.Printf("\nCOMPRESSION SUMMARY\n")
	fmt.Printf("===================\n")

	if compressedMappings == 0 && uncompressedMappings == 0 {
		fmt.Printf("No data mappings (all sparse)\n")

		return
	}

	fmt.Printf("Mappings:          %d compressed, %d uncompressed\n", compressedMappings, uncompressedMappings)

	if compressedMappings > 0 {
		ratio := float64(totalUncompressedBytes) / float64(totalCompressedBytes)
		savings := 100.0 * (1.0 - float64(totalCompressedBytes)/float64(totalUncompressedBytes))
		fmt.Printf("Total frames:      %d\n", totalFrames)
		fmt.Printf("Uncompressed size: %d B (%.2f MiB)\n", totalUncompressedBytes, float64(totalUncompressedBytes)/1024/1024)
		fmt.Printf("Compressed size:   %d B (%.2f MiB)\n", totalCompressedBytes, float64(totalCompressedBytes)/1024/1024)
		fmt.Printf("Compression ratio: %.2fx (%.1f%% space savings)\n", ratio, savings)
	} else {
		fmt.Printf("All mappings are uncompressed\n")
	}

	hasCompressedBuilds := false
	for _, stats := range buildCompressionStats {
		if stats.compressed {
			hasCompressedBuilds = true

			break
		}
	}

	if hasCompressedBuilds {
		fmt.Printf("\nPer-build compression:\n")
		for buildID, stats := range buildCompressionStats {
			label := buildID[:8] + "..."
			if buildID == h.Metadata.BuildId.String() {
				label += " (current)"
			} else if buildID == h.Metadata.BaseBuildId.String() {
				label += " (parent)"
			}

			if stats.compressed {
				ratio := float64(stats.uncompressedBytes) / float64(stats.compressedBytes)
				fmt.Printf("  %s: %d frames, U=%d C=%d (%.2fx)\n",
					label, stats.frames, stats.uncompressedBytes, stats.compressedBytes, ratio)
			} else {
				fmt.Printf("  %s: uncompressed, %d B\n", label, stats.uncompressedBytes)
			}
		}
	}
}
