package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/session"
	kubo "github.com/guseggert/clustertest-kubo"
	"github.com/guseggert/clustertest/cluster"
	"github.com/guseggert/clustertest/cluster/aws"
	"github.com/guseggert/clustertest/cluster/basic"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// example invocation: ./latency --region eu-north-1 --versions v0.17.0,v0.16.0,v0.15.0 --nodes-per-version 5 --settle 10s --urls /ipns/filecoin.io,/ipns/ipfs.io --times 5 --cluster aws
func main() {
	app := &cli.App{
		Name:  "latency",
		Usage: "measures the latency of making requests to the local gateway",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:    "versions",
				Usage:   "the kubo versions to test (comma-separated), e.g. 'v0.16.0,v0.17.0'.",
				Value:   cli.NewStringSlice("v0.17.0"),
				EnvVars: []string{"CLUSTERTEST_VERSIONS"},
			},
			&cli.IntFlag{
				Name:    "nodes-per-version",
				Usage:   "the number of nodes per version to run",
				Value:   1,
				EnvVars: []string{"CLUSTERTEST_NODES_PER_VERSION"},
			},
			&cli.StringSliceFlag{
				Name:    "regions",
				Usage:   "the AWS regions to use, if using an AWS cluster",
				EnvVars: []string{"CLUSTERTEST_REGIONS"},
			},
			&cli.DurationFlag{
				Name:    "settle",
				Usage:   "the duration to wait after all daemons are online before starting the test",
				Value:   10 * time.Second,
				EnvVars: []string{"CLUSTERTEST_SETTLE"},
			},
			&cli.StringSliceFlag{
				Name:     "urls",
				Usage:    "URLs to test against, relative to the gateway URL. Example: '/ipns/ipfs.io'",
				Required: true,
				EnvVars:  []string{"CLUSTERTEST_URLS"},
			},
			&cli.IntFlag{
				Name:    "times",
				Usage:   "number of times to test each URL",
				Value:   5,
				EnvVars: []string{"CLUSTERTEST_TIMES"},
			},
			&cli.BoolFlag{
				Name: "verbose",
			},
			&cli.StringFlag{
				Name:    "nodeagent",
				Usage:   "path to the nodeagent binary",
				Value:   "",
				EnvVars: []string{"CLUSTERTEST_NODEAGENT_BIN"},
			},
			&cli.StringFlag{
				Name:    "db-host",
				Usage:   "On which host address can this clustertest reach the database",
				EnvVars: []string{"CLUSTERTEST_DATABASE_HOST"},
			},
			&cli.IntFlag{
				Name:    "db-port",
				Usage:   "On which port can this clustertest reach the database",
				EnvVars: []string{"CLUSTERTEST_DATABASE_PORT"},
			},
			&cli.StringFlag{
				Name:    "db-name",
				Usage:   "The name of the database to use",
				EnvVars: []string{"CLUSTERTEST_DATABASE_NAME"},
			},
			&cli.StringFlag{
				Name:    "db-password",
				Usage:   "The password for the database to use",
				EnvVars: []string{"CLUSTERTEST_DATABASE_PASSWORD"},
			},
			&cli.StringFlag{
				Name:    "db-user",
				Usage:   "The user with which to access the database to use",
				EnvVars: []string{"CLUSTERTEST_DATABASE_USER"},
			},
			&cli.StringFlag{
				Name:    "db-sslmode",
				Usage:   "The sslmode to use when connecting the the database",
				EnvVars: []string{"CLUSTERTEST_DATABASE_SSL_MODE"},
			},
			&cli.StringSliceFlag{
				Name:    "public-subnet-ids",
				Usage:   "The public subnet IDs to run the cluster in",
				EnvVars: []string{"CLUSTERTEST_PUBLIC_SUBNET_IDS"},
			},
			&cli.StringSliceFlag{
				Name:    "instance-profile-arns",
				Usage:   "The instance profiles to run the Kubo nodes with",
				EnvVars: []string{"CLUSTERTEST_INSTANCE_PROFILE_ARNS"},
			},
			&cli.StringSliceFlag{
				Name:    "instance-security-group-ids",
				Usage:   "The security groups of the Kubo instances",
				EnvVars: []string{"CLUSTERTEST_SECURITY_GROUP_IDS"},
			},
			&cli.StringSliceFlag{
				Name:    "s3-bucket-arns",
				EnvVars: []string{"CLUSTERTEST_S3_BUCKET_ARNS"},
			},
		},
		Action: func(cliCtx *cli.Context) error {

			nodeagent := cliCtx.String("nodeagent")
			verbose := cliCtx.Bool("verbose")
			regions := cliCtx.StringSlice("regions")
			subnetIDs := cliCtx.StringSlice("public-subnet-ids")
			instanceProfileARNs := cliCtx.StringSlice("instance-profile-arns")
			instanceSecurityGroupIDs := cliCtx.StringSlice("instance-security-group-ids")
			s3BucketARNs := cliCtx.StringSlice("s3-bucket-arns")

			var l *zap.Logger
			var err error
			if verbose {
				l, err = zap.NewDevelopment()
			} else {
				l, err = zap.NewProduction()
			}
			if err != nil {
				return fmt.Errorf("initializing logger: %w", err)
			}
			logger := l.Sugar()

			db, err := InitDB(cliCtx)
			if err != nil {
				return fmt.Errorf("initializing database connection: %w", err)
			}
			defer db.Close()

			var clusterImpls []cluster.Cluster
			for idx, region := range regions {
				iparn, err := arn.Parse(instanceProfileARNs[idx])
				if err != nil {
					return fmt.Errorf("error parsing instnace profile arn: %w", err)
				}

				s3arn, err := arn.Parse(s3BucketARNs[idx])
				if err != nil {
					return fmt.Errorf("error parsing s3 bucket arn: %w", err)
				}
				r := region
				clusterImpl := aws.NewCluster().
					WithNodeAgentBin(nodeagent).
					WithSession(session.Must(session.NewSession(&awssdk.Config{Region: &r}))).
					WithLogger(logger).
					WithPublicSubnetID(subnetIDs[idx]).
					WithInstanceProfileARN(iparn).
					WithInstanceSecurityGroupID(instanceSecurityGroupIDs[idx]).
					WithS3BucketARN(s3arn)

				clusterImpls = append(clusterImpls, clusterImpl)
			}

			// For each version, load the Kubo binary, initialize the repo, and run the daemon.
			errg := errgroup.Group{}
			for i, clusterImpl := range clusterImpls {
				ci := clusterImpl
				region := regions[i]
				errg.Go(func() error {
					return runRegion(cliCtx, db, ci, logger, region)
				})
			}

			return errg.Wait()
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func runRegion(cliCtx *cli.Context, db *sql.DB, clus cluster.Cluster, logger *zap.SugaredLogger, region string) error {
	ctx := cliCtx.Context

	versions := cliCtx.StringSlice("versions")
	nodesPerVersion := cliCtx.Int("nodes-per-version")
	urls := cliCtx.StringSlice("urls")
	times := cliCtx.Int("times")
	settle := cliCtx.Duration("settle")
	c := kubo.New(basic.New(clus).WithLogger(logger))
	defer c.Cleanup()

	log.Printf("Launching %d nodes in %s", len(versions)*nodesPerVersion, region)
	nodes := c.MustNewNodes(len(versions) * nodesPerVersion)
	var nodeVersions []string
	for i, v := range versions {
		for j := 0; j < nodesPerVersion; j++ {
			node := nodes[i*nodesPerVersion+j]
			node.WithKuboVersion(v)
			nodeVersions = append(nodeVersions, v)
		}
	}

	group, groupCtx := errgroup.WithContext(ctx)
	for _, node := range nodes {
		node := node
		group.Go(func() error {
			node = node.Context(groupCtx)
			err := node.LoadBinary()
			if err != nil {
				return fmt.Errorf("loading binary: %w", err)
			}
			err = node.Init()
			if err != nil {
				return fmt.Errorf("initializing kubo: %w", err)
			}
			err = node.ConfigureForRemote()
			if err != nil {
				return fmt.Errorf("configuring kubo: %w", err)
			}
			_, err = node.Context(ctx).StartDaemonAndWaitForAPI()
			if err != nil {
				return fmt.Errorf("waiting for kubo to startup: %w", err)
			}
			return nil
		})
	}

	log.Printf("Setting up nodes")
	err := group.Wait()
	if err != nil {
		return fmt.Errorf("waiting on nodes to setup: %w", err)
	}

	log.Printf("Daemons running, waiting to settle...\n")
	time.Sleep(settle)

	var m sync.Mutex
	// map from node to url to list of latencies in attempt order
	results := map[int]map[string][]int64{}

	group, groupCtx = errgroup.WithContext(ctx)
	for i, node := range nodes {
		node := node
		nodeNum := i
		group.Go(func() error {
			node = node.Context(groupCtx)
			gatewayURL, err := node.GatewayURL()
			if err != nil {
				return fmt.Errorf("node %s getting gateway URL: %w", node.Node.Node, err)
			}

			for _, u := range urls {
				reqURL := fmt.Sprintf("http://localhost:%s%s", gatewayURL.Port(), u)
				for i := 0; i < times; i++ {
					loadTime, err := runPhantomas(groupCtx, node, u)
					if err != nil {
						log.Printf("error running phantomas: %s", err)
						continue
					}

					fmt.Printf("region: %s\tnode: %d\turl: %s\treq: %d\tversion: %s\tlatency (ms): %s\n", region, nodeNum, reqURL, i, node.MustVersion(), loadTime)

					m.Lock()
					if results[nodeNum] == nil {
						results[nodeNum] = map[string][]int64{}
					}
					results[nodeNum][u] = append(results[nodeNum][u], loadTime.Milliseconds())
					m.Unlock()

					gcCtx, cancelGC := context.WithTimeout(groupCtx, 10*time.Second)
					err = kubo.ProcMust(node.Context(gcCtx).RunKubo(cluster.StartProcRequest{
						Args: []string{"repo", "gc"},
					}))
					if err != nil {
						cancelGC()
						return fmt.Errorf("%s node %d running gc: %w", region, nodeNum, err)
					}
					cancelGC()
				}
			}
			return nil
		})
	}

	if err = group.Wait(); err != nil {
		return fmt.Errorf("running %s test: %w", region, err)
	}

	for nodeNum := 0; nodeNum < len(nodes); nodeNum++ {
		version := nodeVersions[nodeNum]
		for _, url := range urls {
			times := results[nodeNum][url]
			for _, t := range times {
				fmt.Printf("region=%s\tversion=%s\turl=%s\tnode=%d\tms=%d\n", region, version, url, nodeNum, t)
				_, err = db.ExecContext(ctx, "INSERT INTO website_measurements (region, url, version, node_num, latency, created_at) VALUES ($1, $2, $3, $4, $5, NOW())", region, url, version, nodeNum, float64(t)/1000.0)
				if err != nil {
					fmt.Println("err inserting row:", err)
				}
			}
		}
	}

	return nil
}

type phantomasOutput struct {
	Metrics struct {
		PerformanceTimingPageLoad int
	}
}

func runPhantomas(ctx context.Context, node *kubo.Node, url string) (*time.Duration, error) {
	ctx, cancelCurl := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelCurl()

	gatewayURL, err := node.GatewayURL()
	if err != nil {
		return nil, err
	}

	_, err = node.Run(cluster.StartProcRequest{
		Command: "docker",
		Args: []string{
			"pull",
			"macbre/phantomas:latest",
		},
	})
	if err != nil {
		return nil, err
	}

	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	_, err = node.Run(cluster.StartProcRequest{
		Command: "docker",
		Args: []string{
			"run",
			"--network=host",
			"--privileged",
			"macbre/phantomas:latest",
			"/opt/phantomas/bin/phantomas.js",
			"--timeout=60",
			fmt.Sprintf("--url=%s%s", gatewayURL, url),
		},
		Stdout: stdout,
		Stderr: stderr,
	})

	if err != nil {
		fmt.Printf("stdout: %s\n", stdout)
		fmt.Printf("stderr: %s\n", stderr)
		return nil, err
	}
	out := &phantomasOutput{}
	err = json.Unmarshal(stdout.Bytes(), out)
	if err != nil {
		return nil, err
	}
	loadTime := time.Duration(out.Metrics.PerformanceTimingPageLoad) * time.Millisecond
	return &loadTime, nil
}

func InitDB(c *cli.Context) (*sql.DB, error) {
	connectionString := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		c.String("db-host"),
		c.Int("db-port"),
		c.String("db-name"),
		c.String("db-user"),
		c.String("db-password"),
		c.String("db-sslmode"),
	)

	fmt.Println(strings.ReplaceAll(connectionString, c.String("db-password"), "***"))

	// Open database handle
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, errors.Wrap(err, "opening database")
	}

	// Ping database to verify connection.
	if err = db.Ping(); err != nil {
		return nil, errors.Wrap(err, "pinging database")
	}

	return db, nil
}
