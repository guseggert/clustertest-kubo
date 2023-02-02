package bin

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/Masterminds/semver/v3"
	"golang.org/x/sync/errgroup"
)

type Versions struct {
	m             map[string]VersionInfo
	latest        string
	latestRelease string
}

type VersionInfo struct {
	Version string
	URL     string
	CID     string
	SHA512  []byte
}

type versionJSON struct {
	Platforms map[string]platformJSON
}

type platformJSON struct {
	Archs map[string]archJSON
}

type archJSON struct {
	Link   string
	CID    string
	SHA512 string
}

func (v Versions) download(ctx context.Context, version string, binPath string) error {
	rc, err := v.FetchVersion(ctx, version)
	if err != nil {
		return fmt.Errorf("fetching: %w", err)
	}
	defer rc.Close()

	f, err := os.Create(binPath)
	if err != nil {
		return fmt.Errorf("creating bin: %w", err)
	}
	defer f.Close()
	_, err = io.Copy(f, rc)
	if err != nil {
		f.Close()
		return fmt.Errorf("copying bin: %w", err)
	}

	return nil
}

func (v Versions) get(vers string) (VersionInfo, error) {
	var k string
	switch vers {
	case "latest":
		k = v.latest
	case "latest-release":
		k = v.latestRelease
	default:
		k = vers
	}
	vi, ok := v.m[k]
	if !ok {
		return VersionInfo{}, fmt.Errorf("version %q not found", vers)
	}
	return vi, nil
}

func (v Versions) FetchVersionWithCaching(ctx context.Context, version string) (io.ReadCloser, error) {
	vi, err := v.get(version)
	if err != nil {
		return nil, err
	}
	tempDir := os.TempDir()
	binPath := filepath.Join(tempDir, vi.CID)

	_, err = os.Stat(binPath)
	if err != nil {
		if os.IsNotExist(err) {
			dlErr := v.download(ctx, version, binPath)
			if dlErr != nil {
				return nil, fmt.Errorf("downloading %s to %s: %w", version, binPath, dlErr)
			}
		} else {
			return nil, fmt.Errorf("stat'ing %q: %w", binPath, err)
		}
	}

	f, err := os.Open(binPath)
	if err != nil {
		return nil, fmt.Errorf("opening bin: %w", err)
	}
	return f, err
}

func (v Versions) FetchVersion(ctx context.Context, version string) (io.ReadCloser, error) {
	vi, err := v.get(version)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, vi.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("building req: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching kubo archive: %w", err)
	}
	return ExtractKuboBinary(resp.Body, vi.SHA512), nil
}

func (v Versions) URL(version string) (string, error) {
	vi, err := v.get(version)
	if err != nil {
		return "", err
	}
	return vi.URL, nil
}

// LoadVersions fetches and caches version metadata from dist.ipfs.io about all Kubo release versions.
// Subsequent calls use the in-memory cached metadata.
func LoadVersions(ctx context.Context) (*Versions, error) {
	versionsMut.Lock()
	defer versionsMut.Unlock()
	if versions != nil {
		return versions, nil
	}

	var m sync.Mutex
	vs := &Versions{m: map[string]VersionInfo{}}

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
				sha512Decoded, err := hex.DecodeString(arch.SHA512)
				if err != nil {
					return fmt.Errorf("decoding sha512: %w", err)
				}

				m.Lock()
				vs.m[version] = VersionInfo{
					Version: version,
					URL:     fmt.Sprintf("https://dist.ipfs.tech/kubo/%s%s", version, arch.Link),
					CID:     arch.CID,
					SHA512:  sha512Decoded,
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

	var releases []*semver.Version
	var allVersions []*semver.Version
	for v := range vs.m {
		sv, err := semver.NewVersion(v)
		if err != nil {
			return nil, fmt.Errorf("parsing semver %q: %w", v, err)
		}
		allVersions = append(allVersions, sv)
		if sv.Prerelease() == "" {
			releases = append(releases, sv)
		}
	}
	sort.Sort(sort.Reverse(semver.Collection(allVersions)))
	sort.Sort(sort.Reverse(semver.Collection(releases)))
	if len(releases) > 0 {
		vs.latestRelease = releases[0].Original()
	}
	if len(allVersions) > 0 {
		vs.latest = allVersions[0].Original()
	}

	versions = vs

	return vs, nil
}
