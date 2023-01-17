package bin

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
)

var (
	versionsMut sync.Mutex
	versions    versionMap
)

// Fetcher fetches a Kubo binary and provides a reader for the binary data.
type Fetcher interface {
	// Fetch fetches a Kubo binary. If this returns no error, then the caller should close the reader to prevent leaks.
	Fetch(context.Context) (io.ReadCloser, error)
}

// RemoteFetcher fetches the given Kubo version binary from dist.ipfs.io, optionally caching it on the local disk.
// Caching is useful to avoid downloading the binary for every test run.
type RemoteFetcher struct {
	CacheLocally bool
	Version      string
}

func (r *RemoteFetcher) Fetch(ctx context.Context) (io.ReadCloser, error) {
	vm, err := GetVersions(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching versions: %w", err)
	}

	if r.CacheLocally {
		return vm.FetchVersionWithCaching(ctx, r.Version)
	}

	return vm.FetchVersion(ctx, r.Version)
}

// GetVersions fetches and caches version metadata from dist.ipfs.io about all Kubo release versions.
// Subsequent calls use the in-memory cached metadata.
func GetVersions(ctx context.Context) (versionMap, error) {
	versionsMut.Lock()
	defer versionsMut.Unlock()
	if versions != nil {
		return versions, nil
	}

	var m sync.Mutex
	vmap := versionMap{}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(6)

	group.Go(func() error {
		req, err := http.NewRequestWithContext(groupCtx, http.MethodGet, "https://dist.ipfs.tech/kubo/versions", nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			version := strings.TrimSpace(scanner.Text())
			group.Go(func() error {
				url := fmt.Sprintf("https://dist.ipfs.tech/kubo/%s/dist.json", version)
				req, err := http.NewRequest(http.MethodGet, url, nil)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return err
				}
				var v versionJSON
				err = json.NewDecoder(resp.Body).Decode(&v)
				resp.Body.Close()
				if err != nil {
					return err
				}
				arch := v.Platforms["linux"].Archs["amd64"]
				m.Lock()
				vmap[version] = VersionInfo{
					URL: fmt.Sprintf("https://dist.ipfs.tech/kubo/%s%s", version, arch.Link),
					CID: arch.CID,
				}
				m.Unlock()
				return nil
			})
		}
		return nil
	})
	err := group.Wait()
	if err != nil {
		return nil, err
	}
	versions = vmap

	return versions, nil
}

// LocalFetcher fetches a Kubo archive from a local file.
// This is useful if you are testing against some unreleased Kubo binary.
type LocalFetcher struct {
	BinPath string
}

func (l *LocalFetcher) Fetch(ctx context.Context) (io.ReadCloser, error) {
	return os.Open(l.BinPath)
}
