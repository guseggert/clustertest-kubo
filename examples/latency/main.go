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
	"github.com/guseggert/clustertest/cluster/docker"
	"github.com/guseggert/clustertest/cluster/local"
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
			&cli.StringFlag{
				Name:    "region",
				Usage:   "the AWS region to use, if using an AWS cluster",
				Value:   "us-east-1",
				EnvVars: []string{"CLUSTERTEST_REGION"},
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
			&cli.StringFlag{
				Name:    "cluster",
				Usage:   "the cluster type to use, one of [local,docker,aws]",
				Value:   "docker",
				EnvVars: []string{"CLUSTERTEST_CLUSTER_TYPE"},
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
			&cli.StringFlag{
				Name:    "public-subnet-id",
				Usage:   "The public subnet ID to run the cluster in",
				EnvVars: []string{"CLUSTERTEST_PUBLIC_SUBNET_ID"},
			},
			&cli.StringFlag{
				Name:    "instance-profile-arn",
				Usage:   "The instance profile to run the Kubo nodes with",
				EnvVars: []string{"CLUSTERTEST_INSTANCE_PROFILE_ARN"},
			},
			&cli.StringFlag{
				Name:    "instance-security-group-id",
				Usage:   "The security group of the Kubo instances",
				EnvVars: []string{"CLUSTERTEST_SECURITY_GROUP_ID"},
			},
			&cli.StringFlag{
				Name:    "s3-bucket-arn",
				EnvVars: []string{"CLUSTERTEST_S3_BUCKET_ARN"},
			},
		},
		Action: func(cliCtx *cli.Context) error {
			versions := cliCtx.StringSlice("versions")
			nodesPerVersion := cliCtx.Int("nodes-per-version")
			nodeagent := cliCtx.String("nodeagent")
			urls := cliCtx.StringSlice("urls")
			times := cliCtx.Int("times")
			region := cliCtx.String("region")
			clusterType := cliCtx.String("cluster")
			verbose := cliCtx.Bool("verbose")
			settle := cliCtx.Duration("settle")

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

			ctx := cliCtx.Context

			var clusterImpl cluster.Cluster
			switch clusterType {
			case "local":
				clusterImpl = local.NewCluster()
			case "docker":
				dc, dcErr := docker.NewCluster()
				err = dcErr
				if err == nil {
					dc.WithLogger(logger)
				}
				clusterImpl = dc
			case "aws":
				iparn, err := arn.Parse(cliCtx.String("instance-profile-arn"))
				if err != nil {
					return fmt.Errorf("error parsing instnace profile arn: %w", err)
				}

				s3arn, err := arn.Parse(cliCtx.String("s3-bucket-arn"))
				if err != nil {
					return fmt.Errorf("error parsing s3 bucket arn: %w", err)
				}
				clusterImpl = aws.NewCluster().
					WithNodeAgentBin(nodeagent).
					WithSession(session.Must(session.NewSession(&awssdk.Config{Region: &region}))).
					WithLogger(logger).
					WithPublicSubnetID(cliCtx.String("public-subnet-id")).
					WithInstanceProfileARN(iparn).
					WithInstanceSecurityGroupID(cliCtx.String("instance-security-group-id")).
					WithS3BucketARN(s3arn)
			default:
				return fmt.Errorf("unknown cluster type %q", clusterType)
			}

			c := kubo.New(basic.New(clusterImpl).WithLogger(logger))

			defer c.Cleanup()

			log.Printf("Launching %d nodes", len(versions)*nodesPerVersion)
			nodes := c.MustNewNodes(len(versions) * nodesPerVersion)
			var nodeVersions []string
			for i, v := range versions {
				for j := 0; j < nodesPerVersion; j++ {
					node := nodes[i*nodesPerVersion+j]
					node.WithKuboVersion(v)
					nodeVersions = append(nodeVersions, v)
				}
			}

			// For each version, load the Kubo binary, initialize the repo, and run the daemon.
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
					//err = node.Context(ctx).WaitOnRefreshedRoutingTable()
					//if err != nil {
					//	return fmt.Errorf("waiting for kubo to startup: %w", err)
					//}
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
							loadTime, err := runPhantomas(groupCtx, node)
							if err != nil {
								return fmt.Errorf("running phantomas: %w", err)
							}

							fmt.Printf("node: %d\turl: %s\treq: %d\tversion: %s\tlatency (ms): %s\n", nodeNum, reqURL, i, node.MustVersion(), loadTime)

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
						_, err = db.ExecContext(ctx, "INSERT INTO website_measurements (region, url, version, node_num, latency, created_at) VALUES ($1, $2, $3, $4, $5, NOW())", region, url, version, nodeNum, float64(t)/1000.0)
						if err != nil {
							fmt.Println("err inserting row:", err)
						}
					}
				}
			}

			return nil
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

type phantomasOutput struct {
	Metrics struct {
		PerformanceTimingPageLoad int
	}
}

func runPhantomas(ctx context.Context, node *kubo.Node) (*time.Duration, error) {
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
			fmt.Sprintf("--url=%s/ipns/protocol.ai", gatewayURL),
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
