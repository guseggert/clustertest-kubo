package bin

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/guseggert/clustertest/cluster"
)

type node interface {
	SendFile(ctx context.Context, filePath string, Contents io.Reader) error
	StartProc(ctx context.Context, req cluster.StartProcRequest) (cluster.Process, error)
}

// Loader loads a Kubo bin into the given file on a node.
type Loader interface {
	Load(ctx context.Context, destFile string) error
	Version(context.Context) (string, error)
}

// SendingLoader loads a Kubo binary by sending it to the node from the test runner.
// This can be slower but sometimes is necessary if e.g. testing an unreleased binary when developing.
type SendingLoader struct {
	Node    node
	Fetcher Fetcher
}

func run(ctx context.Context, n node, req cluster.StartProcRequest) error {
	proc, err := n.StartProc(ctx, req)
	if err != nil {
		return fmt.Errorf("starting process: %w", err)
	}

	res, err := proc.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting: %w", err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("non-zero exit code %d", res.ExitCode)
	}
	return nil
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
	err = run(ctx, b.Node, cluster.StartProcRequest{
		Command: "chmod",
		Args:    []string{"+x", destFile},
	})
	return err
}

func (b *SendingLoader) Version(ctx context.Context) (string, error) {
	return b.Fetcher.Version(ctx)
}

// FetchingLoader loads a Kubo archive by having each node download the specified version from dist.ipfs.io.
// For remote clusters, this is much faster than sending the archive to each one from the test runner.
type FetchingLoader struct {
	Vers    string
	Fetcher cluster.Fetcher
	Node    node
}

func (c *FetchingLoader) Load(ctx context.Context, destFile string) error {
	vm, err := LoadVersions(ctx)
	if err != nil {
		return fmt.Errorf("fetching versions: %w", err)
	}
	url, err := vm.URL(c.Vers)
	if err != nil {
		return fmt.Errorf("finding URL: %w", err)
	}

	dir := filepath.Dir(destFile)
	archivePath := filepath.Join(dir, "kubo.tar.gz")

	err = c.Fetcher.Fetch(ctx, url, archivePath)
	if err != nil {
		return fmt.Errorf("fetching %q: %w", url, err)
	}

	err = run(ctx, c.Node, cluster.StartProcRequest{
		Command: "tar",
		Args:    []string{"xzf", archivePath},
		WD:      dir,
	})
	if err != nil {
		return fmt.Errorf("unarchiving: %w", err)
	}
	err = run(ctx, c.Node, cluster.StartProcRequest{
		Command: "mv",
		Args:    []string{filepath.Join(dir, "kubo", "ipfs"), destFile},
	})
	if err != nil {
		return fmt.Errorf("moving kubo bin: %w", err)
	}
	return nil
}

func (c *FetchingLoader) Version(ctx context.Context) (string, error) {
	vm, err := LoadVersions(ctx)
	if err != nil {
		return "", fmt.Errorf("loading versions: %w", err)
	}
	vi, err := vm.get(c.Vers)
	if err != nil {
		return "", err
	}
	return vi.Version, nil
}
