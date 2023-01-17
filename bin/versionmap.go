package bin

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

type versionMap map[string]VersionInfo

type VersionInfo struct {
	URL string
	CID string
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

func (v versionMap) FetchVersionWithCaching(ctx context.Context, version string) (io.ReadCloser, error) {
	vi, ok := v[version]
	if !ok {
		return nil, fmt.Errorf("no such version %q", version)
	}
	tempDir := os.TempDir()
	binPath := filepath.Join(tempDir, vi.CID)

	_, err := os.Stat(binPath)
	if err != nil {
		if os.IsNotExist(err) {
			rc, err := v.FetchVersion(ctx, version)
			if err != nil {
				return nil, fmt.Errorf("fetching: %w", err)
			}
			defer rc.Close()

			f, err := os.Create(binPath)
			if err != nil {
				return nil, fmt.Errorf("creating bin: %w", err)
			}
			_, err = io.Copy(f, rc)
			if err != nil {
				f.Close()
				return nil, fmt.Errorf("copying bin: %w", err)
			}
			f.Close()
		} else {
			return nil, fmt.Errorf("stat'ing %q: %w", binPath, err)
		}
	}
	// TODO checksum
	f, err := os.Open(binPath)
	if err != nil {
		return nil, fmt.Errorf("opening bin: %w", err)
	}
	return f, err
}

func (v versionMap) FetchVersion(ctx context.Context, version string) (io.ReadCloser, error) {
	vi, ok := v[version]
	if !ok {
		return nil, fmt.Errorf("no such version %q", version)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, vi.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("building req: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching kubo archive: %w", err)
	}
	return ExtractKuboBinary(resp.Body), nil
}

func (v versionMap) URL(version string) (string, error) {
	vi, ok := v[version]
	if !ok {
		return "", fmt.Errorf("no such version %q", version)
	}
	return vi.URL, nil
}
