package kubo

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	_ "net/http/pprof"

	"github.com/guseggert/clustertest/cluster"
	"github.com/guseggert/clustertest/cluster/basic"
	"github.com/guseggert/clustertest/cluster/local"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var logger *zap.SugaredLogger

func init() {
	l, err := zap.NewProduction()
	// l, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	logger = l.Sugar()
}

// TestKuboVersions launches a cluster of 4 kubo nodes of different versions, and verifies that they can all connect to each other.
func TestKuboVersions(t *testing.T) {
	ctx := context.Background()
	clusterImpl, err := local.NewCluster()
	// clusterImpl, err := docker.NewCluster("ubuntu")
	// clusterImpl = clusterImpl.WithLogger(logger)
	// clusterImpl, err := aws.NewCluster()
	require.NoError(t, err)

	bc := basic.New(clusterImpl).WithLogger(logger)
	c := New(bc)

	// defer c.Cleanup()

	versions := []string{
		"v0.15.0",
		"v0.16.0",
		"v0.17.0",
		"v0.18.0-rc1",
	}

	nodes := Must2(c.NewNodes(len(versions)))
	for i, node := range nodes {
		nodes[i] = node.WithKuboVersion(versions[i])
	}

	require.NoError(t, err)

	// For each version, load the Kubo binary, initialize the repo, and run the daemon.
	group, groupCtx := errgroup.WithContext(ctx)
	var daemonsMut sync.Mutex
	var daemons []*Daemon
	for _, node := range nodes {
		node := node.Context(groupCtx)
		group.Go(func() error {
			err := node.LoadBinary()
			if err != nil {
				return fmt.Errorf("loading binary: %w", err)
			}
			err = node.Init()
			if err != nil {
				return err
			}

			err = node.ConfigureForRemote()
			if err != nil {
				return err
			}
			daemon, err := node.StartDaemon()
			if err != nil {
				return err
			}

			daemonsMut.Lock()
			daemons = append(daemons, daemon)
			daemonsMut.Unlock()

			err = node.WaitOnAPI()
			if err != nil {
				return err
			}
			return nil
		})
	}
	require.NoError(t, group.Wait())

	// verify that the versions are what we expect
	actualVersions := map[string]bool{}
	for _, node := range nodes {
		apiClient := Must2(node.RPCAPIClient())
		vers, _, err := apiClient.Version()
		require.NoError(t, err)
		actualVersions[vers] = true
	}

	expectedVersions := map[string]bool{
		"0.15.0":     true,
		"0.16.0":     true,
		"0.17.0":     true,
		"0.18.0-rc1": true,
	}

	assert.Equal(t, expectedVersions, actualVersions)

	ensureConnected := func(from *Node, to *Node) {
		connected := false
		httpClient := Must2(from.RPCHTTPClient())
		fromCIs := Must2(httpClient.Swarm().Peers(ctx))
		for _, ci := range fromCIs {
			toAI := Must2(to.AddrInfo())
			if ci.ID() == toAI.ID {
				connected = true
			}
		}
		assert.True(t, connected)
	}

	// Connect every node to every other node, then verify that the connect succeded
	for _, from := range nodes {
		for _, to := range nodes {
			fromAI := Must2(from.AddrInfo())
			toAI := Must2(to.AddrInfo())

			if fromAI.ID == toAI.ID {
				continue
			}

			RemoveLocalAddrs(toAI)

			apiClient := Must2(from.RPCAPIClient())

			err = apiClient.SwarmConnect(ctx, toAI.Addrs[0].String())
			require.NoError(t, err)

			ensureConnected(from, to)
		}
	}

	for _, n := range nodes {
		var stdout bytes.Buffer
		res, err := n.RunKubo(cluster.StartProcRequest{
			Args:   []string{"id"},
			Stdout: &stdout,
		})
		require.NoError(t, err)
		require.Equal(t, 0, res.ExitCode)
	}

	// stop the daemons
	for _, d := range daemons {
		assert.NoError(t, d.Stop())
	}
}
