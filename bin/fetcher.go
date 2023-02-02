package bin

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
)

var (
	versionsMut sync.Mutex
	versions    *Versions
)

// Fetcher fetches a Kubo binary and provides a reader for the binary data.
type Fetcher interface {
	// Fetch fetches a Kubo binary. If this returns no error, then the caller should close the reader to prevent leaks.
	Fetch(context.Context) (io.ReadCloser, error)
	Version(context.Context) (string, error)
}

// DistFetcher fetches the given Kubo version binary from dist.ipfs.io, optionally caching it on the local disk.
// Caching is useful to avoid downloading the binary for every test run.
type DistFetcher struct {
	CacheLocally bool
	Vers         string
}

func (r *DistFetcher) Fetch(ctx context.Context) (io.ReadCloser, error) {
	vm, err := LoadVersions(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching versions: %w", err)
	}

	if r.CacheLocally {
		return vm.FetchVersionWithCaching(ctx, r.Vers)
	}

	return vm.FetchVersion(ctx, r.Vers)
}

func (r *DistFetcher) Version(ctx context.Context) (string, error) {
	vm, err := LoadVersions(ctx)
	if err != nil {
		return "", fmt.Errorf("fetching versions: %w", err)
	}
	vi, err := vm.get(r.Vers)
	if err != nil {
		return "", err
	}
	return vi.Version, nil
}

// DockerImageFetcher fetches a Kubo binary by extracting it from a Docker image.
type DockerImageFetcher struct {
	Image string
}

func (r *DockerImageFetcher) Fetch(ctx context.Context) (io.ReadCloser, error) {
	return nil, nil
}

// RemoteFetcher fetches a Kubo binary from one of multiple remote sources.
// The source used depends on the version passed, which can be of the following schemes:
//
// - dist:<semver>        the binary is loaded from dist.ipfs.io according to the semver, such as v0.18.0 or v0.18.0-rc1
// - dist:latest          the latest semver will be loaded from dist.ipfs.io (including RCs)
// - dist:latest-release  the latest release will be loaded from dist.ipfs.io
// - docker:latest        the binary is loaded from the latest Docker image
// - docker:<commit hash> the binary is loaded from the first Docker image that matches the hash
type RemoteFetcher struct {
}

// LocalFetcher fetches a Kubo archive from a local file.
// This is useful if you are testing against some unreleased Kubo binary.
type LocalFetcher struct {
	BinPath string
}

func (l *LocalFetcher) Fetch(ctx context.Context) (io.ReadCloser, error) {
	return os.Open(l.BinPath)
}

func (l *LocalFetcher) Version(_ context.Context) (string, error) {
	return "local", nil
}
