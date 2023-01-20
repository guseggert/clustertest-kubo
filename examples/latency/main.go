package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	kubo "github.com/guseggert/clustertest-kubo"
	"github.com/guseggert/clustertest/cluster"
	"github.com/guseggert/clustertest/cluster/aws"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func main() {
	app := &cli.App{
		Name:  "latency",
		Usage: "measures the latency of making requests to the local gateway",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:  "versions",
				Usage: "the kubo versions to test (comma-separated), e.g. 'v0.16.0,v0.17.0'.",
				Value: cli.NewStringSlice("v0.17.0"),
			},
			&cli.IntFlag{
				Name:  "nodes-per-version",
				Usage: "the number of nodes per version to run",
				Value: 1,
			},
			&cli.StringFlag{
				Name:     "region",
				Usage:    "the AWS region to use",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "settle",
				Usage: "the duration to wait after all daemons are online before starting the test",
				Value: "10s",
			},
			&cli.StringSliceFlag{
				Name:     "urls",
				Usage:    "URLs to test against, relative to the gateway URL. Example: '/ipns/ipfs.io'",
				Required: true,
			},
			&cli.IntFlag{
				Name:  "times",
				Usage: "number of times to test each URL",
				Value: 5,
			},
		},
		Action: func(cliCtx *cli.Context) error {
			versions := cliCtx.StringSlice("versions")
			nodesPerVersion := cliCtx.Int("nodes-per-version")
			urls := cliCtx.StringSlice("urls")
			times := cliCtx.Int("times")
			region := cliCtx.String("region")
			settleStr := cliCtx.String("settle")
			settle, err := time.ParseDuration(settleStr)
			if err != nil {
				return fmt.Errorf("parsing settle: %w", err)
			}

			// l, err := zap.NewProduction()
			l, err := zap.NewDevelopment()
			if err != nil {
				return fmt.Errorf("initializing logger: %w", err)
			}

			logger := l.Sugar()

			ctx := cliCtx.Context

			// clusterImpl, err := docker.NewCluster("ubuntu")
			clusterImpl, err := aws.NewCluster(aws.Options{
				SessionOptions: &session.Options{
					Config: awssdk.Config{
						Region: &region,
					},
				},
			})
			if err != nil {
				return fmt.Errorf("creating AWS cluster: %w", err)
			}

			bc, err := cluster.New(clusterImpl, cluster.WithLogger(logger))
			if err != nil {
				return fmt.Errorf("creating basic cluster: %w", err)
			}

			c := &kubo.Cluster{BasicCluster: bc}

			defer c.Cleanup(ctx)

			var versionOpts [][]kubo.NodeOption
			var nodeVersions []string
			for _, v := range versions {
				for i := 0; i < nodesPerVersion; i++ {
					versionOpts = append(versionOpts, []kubo.NodeOption{kubo.WithKuboVersion(v)})
					nodeVersions = append(nodeVersions, v)
				}
			}

			log.Printf("Launching %d nodes in %s", len(versionOpts), region)
			nodes, err := c.NewNodesWithOpts(ctx, versionOpts)
			if err != nil {
				return fmt.Errorf("launchign nodes: %w", err)
			}

			// For each version, load the Kubo binary, initialize the repo, and run the daemon.
			group, groupCtx := errgroup.WithContext(ctx)
			daemonCtx, daemonCancel := context.WithCancel(ctx)
			defer daemonCancel()
			daemonGroup, daemonGroupCtx := errgroup.WithContext(daemonCtx)
			for _, node := range nodes {
				node := node
				group.Go(func() error {
					err := node.LoadBinary(groupCtx)
					if err != nil {
						return fmt.Errorf("loading binary: %w", err)
					}
					err = node.Init(groupCtx)
					if err != nil {
						return fmt.Errorf("initializing kubo: %w", err)
					}

					err = node.ConfigureForRemote(groupCtx)
					if err != nil {
						return fmt.Errorf("configuring kubo: %w", err)
					}
					daemonGroup.Go(func() error {
						return node.RunDaemon(daemonGroupCtx)
					})
					err = node.WaitOnAPI(groupCtx)
					if err != nil {
						return fmt.Errorf("waiting for kubo to startup: %w", err)
					}
					return nil
				})
			}
			log.Printf("Setting up nodes")
			err = group.Wait()
			if err != nil {
				return fmt.Errorf("waiting on nodes to setup: %w", err)
			}

			log.Printf("Daemons running, waiting to settle...\n")
			time.Sleep(settle)

			var m sync.Mutex
			// map from node to url to list of latencies in attempt order
			results := map[int]map[string][]int{}

			group, groupCtx = errgroup.WithContext(ctx)
			for i, node := range nodes {
				node := node
				nodeNum := i
				version := nodeVersions[i]
				group.Go(func() error {
					gatewayURL, err := node.GatewayURL(groupCtx)
					if err != nil {
						return fmt.Errorf("node %s getting gateway URL: %w", node, err)
					}

					for _, u := range urls {
						reqURL := fmt.Sprintf("http://localhost:%s%s", gatewayURL.Port(), u)
						for i := 0; i < times; i++ {
							curlCtx, cancelCurl := context.WithTimeout(groupCtx, 5*time.Minute)
							now := time.Now()
							code, err := node.Run(curlCtx, cluster.StartProcRequest{
								Command: "curl",
								Args:    []string{reqURL},
							})
							if err != nil {
								fmt.Printf("node %d error running curl: %s\n", nodeNum, err)
								cancelCurl()
								continue
							}
							if code != 0 {
								fmt.Printf("node %d non-zero exit code %d\n", nodeNum, code)
								cancelCurl()
								continue
							}
							cancelCurl()

							latency := time.Since(now)
							fmt.Printf("node: %d\turl:%s\treq: %d\tversion: %s\tlatency (ms): %d\n", nodeNum, reqURL, i, version, latency.Milliseconds())

							m.Lock()
							if results[nodeNum] == nil {
								results[nodeNum] = map[string][]int{}
							}
							results[nodeNum][u] = append(results[nodeNum][u], int(latency.Milliseconds()))
							m.Unlock()

							gcCtx, cancelGC := context.WithTimeout(groupCtx, 10*time.Second)
							err = node.RunKubo(gcCtx, cluster.StartProcRequest{
								Args: []string{"repo", "gc"},
							})
							if err != nil {
								cancelGC()
								return fmt.Errorf("node %d running gc: %w", nodeNum, err)
							}
							cancelGC()
						}
					}
					return nil
				})
			}
			err = group.Wait()
			if err != nil {
				log.Fatalf("running test: %s", err)
			}

			for nodeNum := 0; nodeNum < len(nodes); nodeNum++ {
				version := nodeVersions[nodeNum]
				for _, url := range urls {
					times := results[nodeNum][url]
					for _, t := range times {
						fmt.Printf("region=%s\tversion=%s\turl=%s\tnode=%d\tms=%d\n", region, version, url, nodeNum, t)
					}
				}
			}

			daemonCancel()
			daemonGroup.Wait()

			return nil
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
