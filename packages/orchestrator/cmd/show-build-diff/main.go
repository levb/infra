package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/orchestrator/cmd/internal/cmdutil"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func main() {
	fromBuild := flag.String("from-build", "", "base build ID")
	toBuild := flag.String("to-build", "", "diff build ID")
	storagePath := flag.String("storage", ".local-build", "storage: local path or gs://bucket")
	memfile := flag.Bool("memfile", false, "inspect memfile artifact")
	rootfs := flag.Bool("rootfs", false, "inspect rootfs artifact")
	compressed := flag.Bool("compressed", false, "read compressed headers (.compressed.header.lz4)")
	summary := flag.Bool("summary", false, "show only diff statistics (skip per-mapping listing)")
	visualize := flag.Bool("visualize", false, "visualize the headers")

	flag.Parse()

	if *fromBuild == "" {
		log.Fatal("-from-build required")
	}
	if *toBuild == "" {
		log.Fatal("-to-build required")
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
		artifactName = storage.MemfileName
	} else {
		artifactName = storage.RootfsName
	}

	ctx := context.Background()

	// Read headers
	var baseHeader, diffHeader *header.Header
	var baseSource, diffSource string

	if *compressed {
		var err error
		baseHeader, baseSource, err = cmdutil.ReadCompressedHeader(ctx, *storagePath, *fromBuild, artifactName)
		if err != nil {
			log.Fatalf("failed to read base compressed header: %s", err)
		}
		if baseHeader == nil {
			log.Fatalf("compressed header not found for base build %s", *fromBuild)
		}
		baseSource += " [compressed]"

		diffHeader, diffSource, err = cmdutil.ReadCompressedHeader(ctx, *storagePath, *toBuild, artifactName)
		if err != nil {
			log.Fatalf("failed to read diff compressed header: %s", err)
		}
		if diffHeader == nil {
			log.Fatalf("compressed header not found for diff build %s", *toBuild)
		}
		diffSource += " [compressed]"
	} else {
		baseTemplate := storage.TemplateFiles{BuildID: *fromBuild}
		diffTemplate := storage.TemplateFiles{BuildID: *toBuild}

		baseHeaderFile := baseTemplate.HeaderPath(artifactName)
		diffHeaderFile := diffTemplate.HeaderPath(artifactName)

		baseData, src, err := cmdutil.ReadHeader(ctx, *storagePath, baseHeaderFile)
		if err != nil {
			log.Fatalf("failed to read base header: %s", err)
		}
		baseSource = src

		diffData, src, err := cmdutil.ReadHeader(ctx, *storagePath, diffHeaderFile)
		if err != nil {
			log.Fatalf("failed to read diff header: %s", err)
		}
		diffSource = src

		baseHeader, err = header.Deserialize(baseData)
		if err != nil {
			log.Fatalf("failed to deserialize base header: %s", err)
		}

		diffHeader, err = header.Deserialize(diffData)
		if err != nil {
			log.Fatalf("failed to deserialize diff header: %s", err)
		}
	}

	fmt.Printf("\nBASE METADATA\n")
	fmt.Printf("Storage path       %s\n", baseSource)
	fmt.Printf("========\n")

	if !*summary {
		for _, mapping := range baseHeader.Mapping {
			fmt.Println(cmdutil.FormatMappingWithCompression(mapping, baseHeader.Metadata.BlockSize))
		}
	} else {
		printDiffSummary("Base", baseHeader)
	}

	if *visualize {
		bottomLayers := header.Layers(baseHeader.Mapping)
		delete(*bottomLayers, baseHeader.Metadata.BaseBuildId)

		fmt.Println("")
		fmt.Println(
			header.Visualize(
				baseHeader.Mapping,
				baseHeader.Metadata.Size,
				baseHeader.Metadata.BlockSize,
				128,
				bottomLayers,
				&map[uuid.UUID]struct{}{
					baseHeader.Metadata.BuildId: {},
				},
			),
		)
	}

	if err := header.ValidateMappings(baseHeader.Mapping, baseHeader.Metadata.Size, baseHeader.Metadata.BlockSize); err != nil {
		log.Fatalf("failed to validate base header: %s", err)
	}

	fmt.Printf("\nDIFF METADATA\n")
	fmt.Printf("Storage path       %s\n", diffSource)
	fmt.Printf("========\n")

	onlyDiffMappings := make([]*header.BuildMap, 0)

	for _, mapping := range diffHeader.Mapping {
		if mapping.BuildId == diffHeader.Metadata.BuildId {
			onlyDiffMappings = append(onlyDiffMappings, mapping)
		}
	}

	if !*summary {
		for _, mapping := range onlyDiffMappings {
			fmt.Println(cmdutil.FormatMappingWithCompression(mapping, baseHeader.Metadata.BlockSize))
		}
	} else {
		printDiffSummary("Diff", diffHeader)
	}

	if *visualize {
		fmt.Println("")
		fmt.Println(
			header.Visualize(
				onlyDiffMappings,
				baseHeader.Metadata.Size,
				baseHeader.Metadata.BlockSize,
				128,
				nil,
				header.Layers(onlyDiffMappings),
			),
		)
	}

	mergedHeader := header.MergeMappings(baseHeader.Mapping, onlyDiffMappings)

	fmt.Printf("\n\nMERGED METADATA\n")
	fmt.Printf("========\n")

	if !*summary {
		for _, mapping := range mergedHeader {
			fmt.Println(cmdutil.FormatMappingWithCompression(mapping, baseHeader.Metadata.BlockSize))
		}
	} else {
		fmt.Printf("Mappings: %d\n", len(mergedHeader))
	}

	if *visualize {
		bottomLayers := header.Layers(baseHeader.Mapping)
		delete(*bottomLayers, baseHeader.Metadata.BaseBuildId)

		fmt.Println("")
		fmt.Println(
			header.Visualize(
				mergedHeader,
				baseHeader.Metadata.Size,
				baseHeader.Metadata.BlockSize,
				128,
				bottomLayers,
				header.Layers(onlyDiffMappings),
			),
		)
	}

	if err := header.ValidateMappings(mergedHeader, baseHeader.Metadata.Size, baseHeader.Metadata.BlockSize); err != nil {
		fmt.Fprintf(os.Stderr, "\n\n[VALIDATION ERROR]: failed to validate merged header: %s", err)
	}
}

// printDiffSummary prints a summary of mapping counts and sizes per build.
func printDiffSummary(label string, h *header.Header) {
	type stats struct {
		blocks     int64
		compressed bool
		totalU     int64
		totalC     int64
	}

	builds := make(map[string]*stats)

	for _, mapping := range h.Mapping {
		buildID := mapping.BuildId.String()
		s, ok := builds[buildID]
		if !ok {
			s = &stats{}
			builds[buildID] = s
		}

		s.blocks += int64(mapping.Length) / int64(h.Metadata.BlockSize)

		if mapping.FrameTable != nil && mapping.FrameTable.CompressionType != storage.CompressionNone {
			s.compressed = true
			for _, frame := range mapping.FrameTable.Frames {
				s.totalU += int64(frame.U)
				s.totalC += int64(frame.C)
			}
		} else {
			s.totalU += int64(mapping.Length)
		}
	}

	fmt.Printf("%s: %d mappings, %d builds\n", label, len(h.Mapping), len(builds))
	for buildID, s := range builds {
		short := buildID[:8] + "..."

		switch {
		case buildID == h.Metadata.BuildId.String():
			short += " (current)"
		case buildID == h.Metadata.BaseBuildId.String():
			short += " (parent)"
		case buildID == cmdutil.NilUUID:
			short = "(sparse)"
		}

		if s.compressed && s.totalC > 0 {
			ratio := float64(s.totalU) / float64(s.totalC)
			fmt.Printf("  %s: %d blocks, U=%.1f MiB, C=%.1f MiB (%.2fx)\n",
				short, s.blocks, float64(s.totalU)/1024/1024, float64(s.totalC)/1024/1024, ratio)
		} else {
			fmt.Printf("  %s: %d blocks, %.1f MiB\n",
				short, s.blocks, float64(s.totalU)/1024/1024)
		}
	}
}
