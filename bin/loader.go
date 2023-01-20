package bin

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/guseggert/clustertest/cluster"
)

// Loader loads a Kubo bin into the given file on a node.
type Loader interface {
	Load(ctx context.Context, destFile string) error
}

// SendingLoader loads a Kubo binary by sending it to the node from the test runner.
// This can be slower but sometimes is necessary if e.g. testing an unreleased binary when developing.
type SendingLoader struct {
	Node    *cluster.BasicNode
	Fetcher Fetcher
}

func (b *SendingLoader) Load(ctx context.Context, destFile string) error {
	rc, err := b.Fetcher.Fetch(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()
	err = b.Node.SendFile(ctx, destFile, rc)
	if err != nil {
		return fmt.Errorf("sending file: %w", err)
	}
	res, err := b.Node.Run(ctx, cluster.StartProcRequest{
		Command: "chmod",
		Args:    []string{"+x", destFile},
	})
	if err != nil {
		return fmt.Errorf("setting mode: %w", err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("non-zero exit code %d setting mode", res.ExitCode)
	}
	return nil
}

// FetchingLoader loads a Kubo archive by having each node download the specified version from dist.ipfs.io.
// For remote clusters, this is much faster than sending the archive to each one from the test runner.
type FetchingLoader struct {
	Version string
	Fetcher cluster.Fetcher
	Node    *cluster.BasicNode
}

func (c *FetchingLoader) Load(ctx context.Context, destFile string) error {
	vm, err := GetVersions(ctx)
	if err != nil {
		return fmt.Errorf("fetching versions: %w", err)
	}
	url, err := vm.URL(c.Version)
	if err != nil {
		return fmt.Errorf("finding URL: %w", err)
	}

	dir := filepath.Dir(destFile)
	archivePath := filepath.Join(dir, "kubo.tar.gz")

	err = c.Fetcher.Fetch(ctx, url, archivePath)
	if err != nil {
		return fmt.Errorf("fetching %q: %w", url, err)
	}

	res, err := c.Node.Run(ctx, cluster.StartProcRequest{
		Command: "tar",
		Args:    []string{"xzf", archivePath},
		WD:      dir,
	})
	if err != nil {
		return fmt.Errorf("unarchiving: %w", err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("non-zero exit code %d for unarchive process", res.ExitCode)
	}
	res, err = c.Node.Run(ctx, cluster.StartProcRequest{
		Command: "mv",
		Args:    []string{filepath.Join(dir, "kubo", "ipfs"), destFile},
	})
	if err != nil {
		return fmt.Errorf("moving kubo bin: %w", err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("non-zero exit code %d when moving kubo bin", res.ExitCode)
	}

	return nil
}
