package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"slices"
	"sort"
	"strings"
	"unsafe"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/orchestrator/cmd/internal/cmdutil"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

const nilUUID = "00000000-0000-0000-0000-000000000000"

func main() {
	build := flag.String("build", "", "build ID")
	template := flag.String("template", "", "template ID or alias (requires E2B_API_KEY)")
	storagePath := flag.String("storage", ".local-build", "storage: local path or gs://bucket")
	memfile := flag.Bool("memfile", false, "inspect memfile artifact")
	rootfs := flag.Bool("rootfs", false, "inspect rootfs artifact")
	data := flag.Bool("data", false, "inspect data blocks (default: header only)")
	start := flag.Int64("start", 0, "start block (only with -data)")
	end := flag.Int64("end", 0, "end block, 0 = all (only with -data)")
	human := flag.Bool("human", false, "human-friendly output with redundant sizes and drilldowns")

	flag.Parse()

	// Resolve build ID from template if provided
	if *template != "" && *build != "" {
		log.Fatal("specify either -build or -template, not both")
	}
	if *template != "" {
		resolvedBuild, err := resolveTemplateID(*template)
		if err != nil {
			log.Fatalf("failed to resolve template: %s", err)
		}
		*build = resolvedBuild
		fmt.Printf("Resolved template %q to build %s\n", *template, *build)
	}
	if *build == "" {
		printUsage()
		os.Exit(1)
	}

	// Determine artifact type
	if !*memfile && !*rootfs {
		*memfile = true // default to memfile
	}
	if *memfile && *rootfs {
		log.Fatal("specify either -memfile or -rootfs, not both")
	}

	var artifactName string
	if *memfile {
		artifactName = "memfile"
	} else {
		artifactName = "rootfs.ext4"
	}

	ctx := context.Background()

	// Read header
	headerFile := artifactName + ".header"
	headerData, headerSource, err := cmdutil.ReadFile(ctx, *storagePath, *build, headerFile)
	if err != nil {
		log.Fatalf("failed to read header: %s", err)
	}

	h, err := header.DeserializeBytes(headerData)
	if err != nil {
		log.Fatalf("failed to deserialize header: %s", err)
	}

	// Print header info
	printHeader(h, headerSource, *human)

	// Print builds info (V4 only)
	if h.Builds != nil {
		printBuilds(h, *human)
	}

	// Validate V4 builds map consistency
	if h.Builds != nil {
		validateBuildsMap(h)
	}

	// If -data flag, also inspect data blocks
	if *data {
		dataFile := artifactName
		inspectData(ctx, *storagePath, *build, dataFile, h, *start, *end, *human)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: inspect-build (-build <uuid> | -template <id-or-alias>) [-storage <path>] [-memfile|-rootfs] [-data [-start N] [-end N]] [-human]\n\n")
	fmt.Fprintf(os.Stderr, "The -template flag requires E2B_API_KEY environment variable.\n")
	fmt.Fprintf(os.Stderr, "Set E2B_DOMAIN for non-production environments.\n\n")
	fmt.Fprintf(os.Stderr, "Flags:\n")
	fmt.Fprintf(os.Stderr, "  -human             human-friendly output with redundant sizes and drilldowns\n\n")
	fmt.Fprintf(os.Stderr, "Examples:\n")
	fmt.Fprintf(os.Stderr, "  inspect-build -build abc123                           # inspect memfile header\n")
	fmt.Fprintf(os.Stderr, "  inspect-build -template base -storage gs://bucket     # inspect by template alias\n")
	fmt.Fprintf(os.Stderr, "  inspect-build -template gtjfpksmxd9ct81x1f8e          # inspect by template ID\n")
	fmt.Fprintf(os.Stderr, "  inspect-build -build abc123 -rootfs                   # inspect rootfs header\n")
	fmt.Fprintf(os.Stderr, "  inspect-build -build abc123 -data                     # inspect memfile header + data\n")
	fmt.Fprintf(os.Stderr, "  inspect-build -build abc123 -rootfs -data -end 100    # inspect rootfs header + first 100 blocks\n")
	fmt.Fprintf(os.Stderr, "  inspect-build -build abc123 -storage gs://bucket      # inspect from GCS\n")
	fmt.Fprintf(os.Stderr, "  inspect-build -build abc123 -human                    # human-friendly output\n")
}

func printHeader(h *header.Header, source string, human bool) {
	// Validate mappings
	err := header.ValidateMappings(h.Mapping, h.Metadata.Size, h.Metadata.BlockSize)
	if err != nil {
		fmt.Printf("\nWARNING: Mapping validation failed!\n%s\n\n", err)
	}

	var headerFormat string
	if h.Metadata.Version >= header.MetadataVersionV4 {
		headerFormat = fmt.Sprintf("V%d (compression-capable)", h.Metadata.Version)
	} else {
		headerFormat = fmt.Sprintf("V%d (legacy)", h.Metadata.Version)
	}

	fmt.Printf("\nMETADATA\n")
	fmt.Printf("========\n")
	fmt.Printf("Source             %s\n", source)
	fmt.Printf("Header format      %s\n", headerFormat)
	fmt.Printf("Version            %d\n", h.Metadata.Version)
	fmt.Printf("Generation         %d\n", h.Metadata.Generation)
	fmt.Printf("Build ID           %s\n", h.Metadata.BuildId)
	fmt.Printf("Base build ID      %s\n", h.Metadata.BaseBuildId)
	if human {
		fmt.Printf("Size               %s (%d B)\n", fmtSize(h.Metadata.Size), h.Metadata.Size)
	} else {
		fmt.Printf("Size               %d B (%s)\n", h.Metadata.Size, fmtSize(h.Metadata.Size))
	}
	fmt.Printf("Block size         %d B\n", h.Metadata.BlockSize)
	fmt.Printf("Blocks             %d\n", (h.Metadata.Size+h.Metadata.BlockSize-1)/h.Metadata.BlockSize)

	totalSize := int64(unsafe.Sizeof(header.BuildMap{})) * int64(len(h.Mapping)) / 1024
	var sizeMessage string
	if totalSize == 0 {
		sizeMessage = "<1 KiB"
	} else {
		sizeMessage = fmt.Sprintf("%d KiB", totalSize)
	}

	fmt.Printf("\nMAPPING (%d maps, uses %s in storage)\n", len(h.Mapping), sizeMessage)
	fmt.Printf("=======\n")

	for _, mapping := range h.Mapping {
		fmt.Println(mapping.Format(h.Metadata.BlockSize))
	}

	fmt.Printf("\nMAPPING SUMMARY\n")
	fmt.Printf("===============\n")

	builds := make(map[string]int64)
	for _, mapping := range h.Mapping {
		builds[mapping.BuildId.String()] += int64(mapping.Length)
	}

	for buildID, size := range builds {
		var additionalInfo string
		switch buildID {
		case h.Metadata.BuildId.String():
			additionalInfo = " (current)"
		case h.Metadata.BaseBuildId.String():
			additionalInfo = " (parent)"
		case nilUUID:
			additionalInfo = " (sparse)"
		}
		if human {
			fmt.Printf("%s%s: %d blocks, %s (%d B, %0.2f%%)\n", buildID, additionalInfo, uint64(size)/h.Metadata.BlockSize, fmtSize(uint64(size)), size, float64(size)/float64(h.Metadata.Size)*100)
		} else {
			fmt.Printf("%s%s: %d blocks, %d MiB (%0.2f%%)\n", buildID, additionalInfo, uint64(size)/h.Metadata.BlockSize, uint64(size)/1024/1024, float64(size)/float64(h.Metadata.Size)*100)
		}
	}
}

func printBuilds(h *header.Header, human bool) {
	fmt.Printf("\nBUILDS (%d entries)\n", len(h.Builds))
	fmt.Printf("==================\n")

	// Sort build IDs for deterministic output.
	buildIDs := make([]uuid.UUID, 0, len(h.Builds))
	for id := range h.Builds {
		buildIDs = append(buildIDs, id)
	}
	sort.Slice(buildIDs, func(i, j int) bool {
		return buildIDs[i].String() < buildIDs[j].String()
	})

	if human {
		printBuildsHuman(h, buildIDs)
	} else {
		printBuildsDefault(h, buildIDs)
	}
}

func printBuildsDefault(h *header.Header, buildIDs []uuid.UUID) {
	for _, id := range buildIDs {
		bd := h.Builds[id]
		role := buildRole(id, h.Metadata)

		var parts []string
		parts = append(parts, id.String())
		parts = append(parts, role)

		if bd.FrameData.IsCompressed() {
			ct := bd.FrameData.CompressionType()
			uSize := bd.FrameData.UncompressedSize()
			cSize := bd.FrameData.CompressedSize()
			ratio := float64(uSize) / float64(cSize)
			nFrames := bd.FrameData.NumFrames()

			var frameSize int64
			if nFrames > 0 {
				_, endU, _, _ := bd.FrameData.FrameAt(0)
				startU := int64(0)
				frameSize = endU - startU
			}

			parts = append(parts, ct.String())
			parts = append(parts, fmt.Sprintf("%s -> %s", fmtSize(uint64(uSize)), fmtSize(uint64(cSize))))
			parts = append(parts, fmt.Sprintf("%.2fx", ratio))
			if frameSize > 0 {
				parts = append(parts, fmt.Sprintf("%d frames x %s", nFrames, fmtSize(uint64(frameSize))))
			} else {
				parts = append(parts, fmt.Sprintf("%d frames", nFrames))
			}
		} else {
			parts = append(parts, "none")
			if bd.Size > 0 {
				parts = append(parts, fmtSize(uint64(bd.Size)))
			} else {
				parts = append(parts, "-")
			}
		}

		parts = append(parts, fmtChecksum(bd.Checksum))

		fmt.Printf("  %s\n", strings.Join(parts, "  "))
	}
}

func printBuildsHuman(h *header.Header, buildIDs []uuid.UUID) {
	for _, id := range buildIDs {
		bd := h.Builds[id]
		role := buildRole(id, h.Metadata)

		if bd.FrameData.IsCompressed() {
			ct := bd.FrameData.CompressionType()
			uSize := bd.FrameData.UncompressedSize()
			cSize := bd.FrameData.CompressedSize()
			ratio := float64(uSize) / float64(cSize)
			saved := (1.0 - float64(cSize)/float64(uSize)) * 100
			nFrames := bd.FrameData.NumFrames()

			var frameSizeStr string
			if nFrames > 0 {
				_, endU, _, _ := bd.FrameData.FrameAt(0)
				frameSizeStr = fmtSize(uint64(endU))
			}

			fmt.Printf("\n%s (%s, %s):\n", id, role, ct.String())
			fmt.Printf("  Uncompressed     %s (%d B)\n", fmtSize(uint64(uSize)), uSize)
			fmt.Printf("  Compressed       %s (%d B)\n", fmtSize(uint64(cSize)), cSize)
			fmt.Printf("  Ratio            %.2fx (saved %.1f%%)\n", ratio, saved)
			if frameSizeStr != "" {
				fmt.Printf("  Frames           %d x %s\n", nFrames, frameSizeStr)
			} else {
				fmt.Printf("  Frames           %d\n", nFrames)
			}
			fmt.Printf("  Checksum         %s\n", fmtChecksumLong(bd.Checksum))

			// Compression bar
			pct := float64(cSize) / float64(uSize) * 100
			barWidth := 30
			filled := int(pct / 100 * float64(barWidth))
			if filled > barWidth {
				filled = barWidth
			}
			bar := strings.Repeat("=", filled) + strings.Repeat(" ", barWidth-filled)
			fmt.Printf("  Compression      [%s] %.0f%% of original\n", bar, pct)
		} else {
			fmt.Printf("\n%s (%s, uncompressed):\n", id, role)
			if bd.Size > 0 {
				fmt.Printf("  Size             %s (%d B)\n", fmtSize(uint64(bd.Size)), bd.Size)
			}
			fmt.Printf("  Checksum         %s\n", fmtChecksumLong(bd.Checksum))
		}
	}
}

func validateBuildsMap(h *header.Header) {
	// Check that every non-nil build ID in mappings has a Builds entry.
	referenced := make(map[uuid.UUID]struct{})
	for _, m := range h.Mapping {
		if m.BuildId != uuid.Nil {
			referenced[m.BuildId] = struct{}{}
		}
	}

	for id := range referenced {
		if _, ok := h.Builds[id]; !ok {
			fmt.Printf("\nWARNING: build %s is referenced in mappings but has no entry in Builds map\n", id)
		}
	}

	// Check for unreferenced builds.
	for id := range h.Builds {
		if _, ok := referenced[id]; !ok {
			fmt.Printf("\nWARNING: build %s exists in Builds map but is not referenced by any mapping\n", id)
		}
	}
}

func inspectData(ctx context.Context, storagePath, buildID, dataFile string, h *header.Header, start, end int64, human bool) {
	// Resolve compressed filename if needed.
	if h.Builds != nil {
		if bd, ok := h.Builds[h.Metadata.BuildId]; ok && bd.FrameData.IsCompressed() {
			ct := bd.FrameData.CompressionType()
			compressedFile := dataFile + ct.Suffix()

			fmt.Printf("\nDATA (compressed: %s)\n", ct.String())
			fmt.Printf("====\n")
			fmt.Printf("Data file is compressed (%s); showing frame summary instead of block scan.\n", ct.String())
			fmt.Printf("Storage file       %s\n", compressedFile)

			inspectFrames(ctx, storagePath, buildID, compressedFile, bd, human)

			return
		}
	}

	// Uncompressed path — original logic.
	blockSize := int64(h.Metadata.BlockSize)

	reader, size, source, err := cmdutil.OpenDataFile(ctx, storagePath, buildID, dataFile)
	if err != nil {
		log.Fatalf("failed to open data: %s", err)
	}

	// Validate bounds before defer to avoid exitAfterDefer lint error
	maxBlock := size / blockSize
	if start > maxBlock {
		reader.Close()
		log.Fatalf("start block %d is out of bounds (maximum is %d)", start, maxBlock)
	}
	if end == 0 {
		end = maxBlock
	}
	if end > maxBlock {
		reader.Close()
		log.Fatalf("end block %d is out of bounds (maximum is %d)", end, maxBlock)
	}
	if start > end {
		reader.Close()
		log.Fatalf("start block %d is greater than end block %d", start, end)
	}

	fmt.Printf("\nDATA\n")
	fmt.Printf("====\n")
	fmt.Printf("Source             %s\n", source)
	fmt.Printf("Size               %d B (%s)\n", size, fmtSize(uint64(size)))

	b := make([]byte, blockSize)
	emptyCount := 0
	nonEmptyCount := 0

	fmt.Printf("\nBLOCKS\n")
	fmt.Printf("======\n")

	for i := start * blockSize; i < end*blockSize; i += blockSize {
		_, err := reader.ReadAt(b, i)
		if err != nil {
			reader.Close()
			log.Fatalf("failed to read block: %s", err)
		}

		nonZeroCount := blockSize - int64(bytes.Count(b, []byte("\x00")))

		if nonZeroCount > 0 {
			nonEmptyCount++
			fmt.Printf("%-10d [%11d,%11d) %d non-zero bytes\n", i/blockSize, i, i+blockSize, nonZeroCount)
		} else {
			emptyCount++
			fmt.Printf("%-10d [%11d,%11d) EMPTY\n", i/blockSize, i, i+blockSize)
		}
	}

	fmt.Printf("\nDATA SUMMARY\n")
	fmt.Printf("============\n")
	fmt.Printf("Empty blocks: %d\n", emptyCount)
	fmt.Printf("Non-empty blocks: %d\n", nonEmptyCount)
	fmt.Printf("Total blocks inspected: %d\n", emptyCount+nonEmptyCount)
	if human {
		fmt.Printf("Total size inspected: %s (%d B)\n", fmtSize(uint64(emptyCount+nonEmptyCount)*uint64(blockSize)), int64(emptyCount+nonEmptyCount)*blockSize)
		fmt.Printf("Empty size: %s (%d B)\n", fmtSize(uint64(emptyCount)*uint64(blockSize)), int64(emptyCount)*blockSize)
	} else {
		fmt.Printf("Total size inspected: %d B (%s)\n", int64(emptyCount+nonEmptyCount)*blockSize, fmtSize(uint64(emptyCount+nonEmptyCount)*uint64(blockSize)))
		fmt.Printf("Empty size: %d B (%s)\n", int64(emptyCount)*blockSize, fmtSize(uint64(emptyCount)*uint64(blockSize)))
	}

	reader.Close()
}

func inspectFrames(_ context.Context, _ string, _ string, _ string, bd header.BuildData, human bool) {
	ft := bd.FrameData
	n := ft.NumFrames()
	uTotal := ft.UncompressedSize()
	cTotal := ft.CompressedSize()
	ratio := float64(uTotal) / float64(cTotal)

	var minC, maxC int64
	minC = math.MaxInt64
	for i := range n {
		_, _, startC, endC := ft.FrameAt(i)
		frameC := endC - startC
		if frameC < minC {
			minC = frameC
		}
		if frameC > maxC {
			maxC = frameC
		}
	}
	avgC := cTotal / int64(n)

	fmt.Printf("\nFRAME SUMMARY\n")
	fmt.Printf("=============\n")
	fmt.Printf("Frames             %d\n", n)
	if human {
		fmt.Printf("Uncompressed       %s (%d B)\n", fmtSize(uint64(uTotal)), uTotal)
		fmt.Printf("Compressed         %s (%d B)\n", fmtSize(uint64(cTotal)), cTotal)
		fmt.Printf("Ratio              %.2fx (saved %.1f%%)\n", ratio, (1.0-float64(cTotal)/float64(uTotal))*100)
		fmt.Printf("Frame compressed   min %s, avg %s, max %s\n", fmtSize(uint64(minC)), fmtSize(uint64(avgC)), fmtSize(uint64(maxC)))
	} else {
		fmt.Printf("Uncompressed       %d B (%s)\n", uTotal, fmtSize(uint64(uTotal)))
		fmt.Printf("Compressed         %d B (%s)\n", cTotal, fmtSize(uint64(cTotal)))
		fmt.Printf("Ratio              %.2fx\n", ratio)
		fmt.Printf("Frame compressed   min %d B, avg %d B, max %d B\n", minC, avgC, maxC)
	}
}

// --- helpers ---

func fmtSize(b uint64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func fmtChecksum(cs [32]byte) string {
	if cs == [32]byte{} {
		return "unknown"
	}

	return "sha256:" + hex.EncodeToString(cs[:4]) + "..."
}

func fmtChecksumLong(cs [32]byte) string {
	if cs == [32]byte{} {
		return "unknown"
	}

	return "sha256:" + hex.EncodeToString(cs[:])
}

func buildRole(id uuid.UUID, meta *header.Metadata) string {
	switch {
	case id == uuid.Nil:
		return "sparse"
	case id == meta.BuildId:
		return "current"
	case id == meta.BaseBuildId:
		return "parent"
	default:
		return "ancestor"
	}
}

// templateInfo represents a template from the E2B API.
type templateInfo struct {
	TemplateID string   `json:"templateID"`
	BuildID    string   `json:"buildID"`
	Aliases    []string `json:"aliases"`
	Names      []string `json:"names"`
}

// resolveTemplateID fetches the build ID for a template from the E2B API.
// Input can be a template ID, alias, or full name (e.g., "e2b/base").
func resolveTemplateID(input string) (string, error) {
	apiKey := os.Getenv("E2B_API_KEY")
	if apiKey == "" {
		return "", errors.New("E2B_API_KEY environment variable required for -template flag")
	}

	// Determine API URL
	apiURL := "https://api.e2b.dev/templates"
	if domain := os.Getenv("E2B_DOMAIN"); domain != "" {
		apiURL = fmt.Sprintf("https://api.%s/templates", domain)
	}

	// Make HTTP request
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("X-API-Key", apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to fetch templates: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		return "", fmt.Errorf("API returned %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var templates []templateInfo
	if err := json.NewDecoder(resp.Body).Decode(&templates); err != nil {
		return "", fmt.Errorf("failed to parse API response: %w", err)
	}

	// Find matching template
	var match *templateInfo
	var availableAliases []string

	for i := range templates {
		t := &templates[i]

		// Collect aliases for error message
		availableAliases = append(availableAliases, t.Aliases...)

		// Match by template ID
		if t.TemplateID == input {
			match = t

			break
		}

		// Match by alias
		if slices.Contains(t.Aliases, input) {
			match = t

			break
		}

		// Match by full name (e.g., "e2b/base")
		if slices.Contains(t.Names, input) {
			match = t

			break
		}
	}

	if match == nil {
		return "", fmt.Errorf("template %q not found. Available aliases: %s", input, strings.Join(availableAliases, ", "))
	}

	if match.BuildID == "" || match.BuildID == nilUUID {
		return "", fmt.Errorf("template %q has no successful build", input)
	}

	return match.BuildID, nil
}
