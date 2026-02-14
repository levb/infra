package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"sync/atomic"

	googleStorage "cloud.google.com/go/storage"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

type Destination struct {
	Path    string
	CRC     uint32
	isLocal bool
}

func NewDestinationFromObject(ctx context.Context, o *googleStorage.ObjectHandle) (*Destination, error) {
	var crc uint32
	if attrs, err := o.Attrs(ctx); err == nil {
		crc = attrs.CRC32C
	} else if !errors.Is(err, googleStorage.ErrObjectNotExist) {
		return nil, fmt.Errorf("failed to get object attributes: %w", err)
	}

	return &Destination{
		Path:    fmt.Sprintf("gs://%s/%s", o.BucketName(), o.ObjectName()),
		CRC:     crc,
		isLocal: false,
	}, nil
}

func NewDestinationFromPath(prefix, file string) (*Destination, error) {
	// Local storage uses templates subdirectory
	p := path.Join(prefix, "templates", file)

	if _, err := os.Stat(p); err == nil {
		f, err := os.Open(p)
		if err != nil {
			return nil, fmt.Errorf("failed to open file: %w", err)
		}
		defer f.Close()

		h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
		_, err = io.Copy(h, f)
		if err != nil {
			return nil, fmt.Errorf("failed to copy file: %w", err)
		}
		crc := h.Sum32()

		return &Destination{
			Path:    p,
			CRC:     crc,
			isLocal: true,
		}, nil
	}

	return &Destination{
		Path:    p,
		isLocal: true,
	}, nil
}

func NewHeaderFromObject(ctx context.Context, bucketName string, headerPath string) (*header.Header, error) {
	s, err := storage.NewGCP(ctx, bucketName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS bucket storage provider: %w", err)
	}

	data, err := s.GetBlob(ctx, headerPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open object: %w", err)
	}

	h, err := header.Deserialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize header: %w", err)
	}

	return h, nil
}

func NewHeaderFromPath(_ context.Context, from, headerPath string) (*header.Header, error) {
	data, err := os.ReadFile(path.Join(from, headerPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	h, err := header.Deserialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize header: %w", err)
	}

	return h, nil
}

func getReferencedData(h *header.Header, objectType storage.ObjectType) []string {
	builds := make(map[string]struct{})

	for _, mapping := range h.Mapping {
		builds[mapping.BuildId.String()] = struct{}{}
	}

	delete(builds, uuid.Nil.String())

	var dataReferences []string

	for build := range builds {
		template := storage.TemplateFiles{
			BuildID: build,
		}

		switch objectType {
		case storage.MemfileHeaderObjectType:
			dataReferences = append(dataReferences, template.Path(storage.MemfileName))
		case storage.RootFSHeaderObjectType:
			dataReferences = append(dataReferences, template.Path(storage.RootfsName))
		}
	}

	return dataReferences
}

// getCompressedDataReferences returns compressed data paths (.zst) for all builds referenced by the header.
func getCompressedDataReferences(h *header.Header, objectType storage.ObjectType) []string {
	builds := make(map[string]struct{})

	for _, mapping := range h.Mapping {
		builds[mapping.BuildId.String()] = struct{}{}
	}

	delete(builds, uuid.Nil.String())

	var refs []string

	for build := range builds {
		tmpl := storage.TemplateFiles{BuildID: build}

		switch objectType {
		case storage.MemfileHeaderObjectType:
			refs = append(refs, tmpl.CompressedPath(storage.MemfileName))
		case storage.RootFSHeaderObjectType:
			refs = append(refs, tmpl.CompressedPath(storage.RootfsName))
		}
	}

	return refs
}

func localCopy(ctx context.Context, from, to *Destination) error {
	command := []string{
		"rsync",
		"-aH",
		"--whole-file",
		"--mkpath",
		"--inplace",
		from.Path,
		to.Path,
	}

	cmd := exec.CommandContext(ctx, command[0], command[1:]...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to copy local file (%v): %w\n%s", command, err, string(output))
	}

	return nil
}

func gcloudCopy(ctx context.Context, from, to *Destination) error {
	command := []string{
		"gcloud",
		"storage",
		"cp",
		"--verbosity",
		"error",
		from.Path,
		to.Path,
	}

	cmd := exec.CommandContext(ctx, command[0], command[1:]...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to copy GCS object (%v): %w\n%s", command, err, string(output))
	}

	return nil
}

func main() {
	buildId := flag.String("build", "", "build id")
	from := flag.String("from", "", "from destination")
	to := flag.String("to", "", "to destination")
	skipCompressed := flag.Bool("skip-compressed", false, "skip compressed variants (.zst data, .compressed.header.lz4)")

	flag.Parse()

	fmt.Printf("Copying build '%s' from '%s' to '%s'\n", *buildId, *from, *to)

	tmpl := storage.TemplateFiles{
		BuildID: *buildId,
	}

	ctx := context.Background()

	var filesToCopy []string
	var optionalFiles []string // files that may not exist (compressed variants)

	// Extract all files referenced by the build memfile header
	buildMemfileHeaderPath := tmpl.HeaderPath(storage.MemfileName)

	var memfileHeader *header.Header
	if strings.HasPrefix(*from, "gs://") {
		bucketName, _ := strings.CutPrefix(*from, "gs://")

		h, err := NewHeaderFromObject(ctx, bucketName, buildMemfileHeaderPath)
		if err != nil {
			log.Fatalf("failed to create header from object: %s", err)
		}

		memfileHeader = h
	} else {
		h, err := NewHeaderFromPath(ctx, *from, buildMemfileHeaderPath)
		if err != nil {
			log.Fatalf("failed to create header from path: %s", err)
		}

		memfileHeader = h
	}

	dataReferences := getReferencedData(memfileHeader, storage.MemfileHeaderObjectType)

	filesToCopy = append(filesToCopy, buildMemfileHeaderPath)
	filesToCopy = append(filesToCopy, dataReferences...)

	// Extract all files referenced by the build rootfs header
	buildRootfsHeaderPath := tmpl.HeaderPath(storage.RootfsName)

	var rootfsHeader *header.Header
	if strings.HasPrefix(*from, "gs://") {
		bucketName, _ := strings.CutPrefix(*from, "gs://")
		h, err := NewHeaderFromObject(ctx, bucketName, buildRootfsHeaderPath)
		if err != nil {
			log.Fatalf("failed to create header from object: %s", err)
		}

		rootfsHeader = h
	} else {
		h, err := NewHeaderFromPath(ctx, *from, buildRootfsHeaderPath)
		if err != nil {
			log.Fatalf("failed to create header from path: %s", err)
		}

		rootfsHeader = h
	}

	dataReferences = getReferencedData(rootfsHeader, storage.RootFSHeaderObjectType)

	filesToCopy = append(filesToCopy, buildRootfsHeaderPath)
	filesToCopy = append(filesToCopy, dataReferences...)

	// Add compressed variants (optional — may not exist for older builds)
	if !*skipCompressed {
		// Compressed headers for this build
		optionalFiles = append(optionalFiles,
			tmpl.CompressedHeaderPath(storage.MemfileName),
			tmpl.CompressedHeaderPath(storage.RootfsName),
		)

		// Compressed data files for all referenced builds
		optionalFiles = append(optionalFiles, getCompressedDataReferences(memfileHeader, storage.MemfileHeaderObjectType)...)
		optionalFiles = append(optionalFiles, getCompressedDataReferences(rootfsHeader, storage.RootFSHeaderObjectType)...)
	}

	// Add the snapfile to the list of files to copy
	snapfilePath := tmpl.Path(storage.SnapfileName)
	filesToCopy = append(filesToCopy, snapfilePath)

	metadataPath := tmpl.Path(storage.MetadataName)
	filesToCopy = append(filesToCopy, metadataPath)

	// Combine required and optional files
	optionalSet := make(map[string]struct{}, len(optionalFiles))
	for _, f := range optionalFiles {
		optionalSet[f] = struct{}{}
	}

	allFiles := append(filesToCopy, optionalFiles...)

	// sort files to copy
	sort.Strings(allFiles)

	googleStorageClient, err := googleStorage.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to create Google Storage client: %s", err)
	}

	fmt.Printf("Copying %d files (%d required, %d optional)\n", len(allFiles), len(filesToCopy), len(optionalFiles))

	var eg errgroup.Group

	eg.SetLimit(20)

	var copied, skipped, notFound atomic.Int32

	for _, file := range allFiles {
		_, isOptional := optionalSet[file]

		eg.Go(func() error {
			var fromDestination *Destination
			if strings.HasPrefix(*from, "gs://") {
				bucketName, _ := strings.CutPrefix(*from, "gs://")
				fromObject := googleStorageClient.Bucket(bucketName).Object(file)
				d, destErr := NewDestinationFromObject(ctx, fromObject)
				if destErr != nil {
					if isOptional {
						notFound.Add(1)

						return nil
					}

					return fmt.Errorf("failed to create destination from object: %w", destErr)
				}

				// For optional GCS files, CRC=0 means the object doesn't exist
				if isOptional && d.CRC == 0 {
					notFound.Add(1)

					return nil
				}

				fromDestination = d
			} else {
				d, destErr := NewDestinationFromPath(*from, file)
				if destErr != nil {
					if isOptional {
						notFound.Add(1)

						return nil
					}

					return fmt.Errorf("failed to create destination from path: %w", destErr)
				}

				// For optional local files, CRC=0 means the file doesn't exist
				if isOptional && d.CRC == 0 {
					notFound.Add(1)

					return nil
				}

				fromDestination = d
			}

			var toDestination *Destination
			if strings.HasPrefix(*to, "gs://") {
				bucketName, _ := strings.CutPrefix(*to, "gs://")
				toObject := googleStorageClient.Bucket(bucketName).Object(file)
				d, destErr := NewDestinationFromObject(ctx, toObject)
				if destErr != nil {
					return fmt.Errorf("failed to create destination from object: %w", destErr)
				}

				toDestination = d
			} else {
				d, destErr := NewDestinationFromPath(*to, file)
				if destErr != nil {
					return fmt.Errorf("failed to create destination from path: %w", destErr)
				}

				toDestination = d

				mkdirErr := os.MkdirAll(path.Dir(toDestination.Path), 0o755)
				if mkdirErr != nil {
					return fmt.Errorf("failed to create directory: %w", mkdirErr)
				}
			}

			if fromDestination.CRC == toDestination.CRC && fromDestination.CRC != 0 {
				fmt.Printf("-> '%s' already exists, skipping\n", toDestination.Path)

				skipped.Add(1)

				return nil
			}

			fmt.Printf("+ copying '%s' to '%s'\n", fromDestination.Path, toDestination.Path)

			if fromDestination.isLocal && toDestination.isLocal {
				err := localCopy(ctx, fromDestination, toDestination)
				if err != nil {
					return fmt.Errorf("failed to copy local file: %w", err)
				}
			} else {
				err := gcloudCopy(ctx, fromDestination, toDestination)
				if err != nil {
					return fmt.Errorf("failed to copy GCS object: %w", err)
				}
			}

			copied.Add(1)

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		log.Fatalf("failed to copy files: %s", err)
	}

	fmt.Printf("\nSummary: %d copied, %d skipped (already exist), %d not found (optional)\n",
		copied.Load(), skipped.Load(), notFound.Load())
	fmt.Printf("Build '%s' copied to '%s'\n", *buildId, *to)
}
