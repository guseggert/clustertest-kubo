package kubo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/guseggert/clustertest-kubo/bin"
	"github.com/guseggert/clustertest/cluster"
	clusteriface "github.com/guseggert/clustertest/cluster"
	"github.com/guseggert/clustertest/cluster/basic"
	"github.com/guseggert/clustertest/cluster/docker"
	"github.com/guseggert/clustertest/cluster/local"
	shell "github.com/ipfs/go-ipfs-api"
	httpapi "github.com/ipfs/go-ipfs-http-client"
	"github.com/ipfs/kubo/config"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
)

type Node struct {
	*basic.Node

	Ctx                   context.Context
	Log                   *zap.SugaredLogger
	HTTPClient            *http.Client
	BinLoader             bin.Loader
	APIAvailableSince     time.Time
	GatewayAvailableSince time.Time

	// cached data
	apiAddr       multiaddr.Multiaddr
	rpcAPIClient  *shell.Shell
	rcpHTTPClient *httpapi.HttpApi
	gatewayAddr   *url.URL
}

func (n *Node) WithNodeLogger(l *zap.SugaredLogger) *Node {
	n.Log = l.Named("kubo_node")
	return n
}

// WithLocalKuboBin configures the node to use a Kubo binary on the local filesystem.
func (n *Node) WithLocalKuboBin(binPath string) *Node {
	n.BinLoader = &bin.SendingLoader{
		Node: n.Node.Node,
		Fetcher: &bin.LocalFetcher{
			BinPath: binPath,
		},
	}
	return n
}

func (n *Node) Context(ctx context.Context) *Node {
	newN := *n
	newN.Ctx = ctx
	newN.Node = newN.Node.Context(ctx)
	return &newN
}

// WithKuboVersion configures the node to use a specific version of Kubo from dist.ipfs.io.
func (n *Node) WithKuboVersion(version string) *Node {
	nodeImpl := n.Node.Node
	_, isLocalNode := nodeImpl.(*local.Node)
	_, isDockerNode := nodeImpl.(*docker.Node)
	cacheLocally := isLocalNode || isDockerNode

	if fetcher, ok := nodeImpl.(clusteriface.Fetcher); ok && !cacheLocally {
		n.BinLoader = &bin.FetchingLoader{
			Vers:    version,
			Fetcher: fetcher,
			Node:    nodeImpl,
		}
	} else {
		n.BinLoader = &bin.SendingLoader{
			Node: nodeImpl,
			Fetcher: &bin.DistFetcher{
				CacheLocally: cacheLocally,
				Vers:         version,
			},
		}
	}
	return n
}

func newNode(basicNode *basic.Node) *Node {
	newTransport := http.DefaultTransport.(*http.Transport).Clone()
	newTransport.DialContext = basicNode.Node.Dial
	httpClient := http.Client{Transport: newTransport}

	kn := &Node{
		Node:       basicNode,
		HTTPClient: &httpClient,
		Ctx:        context.Background(),
	}

	return kn.WithNodeLogger(defaultLogger).WithKuboVersion(defaultKuboVersion)
}

func (n *Node) IPFSBin() string {
	return filepath.Join(n.RootDir(), "ipfs")
}

func (n *Node) LoadBinary() error {
	return n.BinLoader.Load(n.Ctx, n.IPFSBin())
}

func (n *Node) Version() (string, error) {
	return n.BinLoader.Version(n.Ctx)
}

func (n *Node) MustVersion() string {
	return Must2(n.Version())
}

type Daemon struct {
	Proc   *basic.Process
	Stdout *bytes.Buffer
	Stderr *bytes.Buffer
}

func (d *Daemon) Stop() error {
	err := d.Proc.Signal(syscall.SIGKILL)
	if err != nil {
		return fmt.Errorf("signaling daemon: %w", err)
	}
	_, err = d.Proc.Wait()
	return err
}

type StopFunc func()

func (n *Node) StartDaemon() (*Daemon, error) {
	stderr := &bytes.Buffer{}
	stdout := &bytes.Buffer{}
	proc, err := n.StartProc(cluster.StartProcRequest{
		Env:     []string{"IPFS_PATH=" + n.IPFSPath()},
		Command: n.IPFSBin(),
		Args:    []string{"daemon"},
		Stdout:  stdout,
		Stderr:  stderr,
	})

	if err != nil {
		return nil, fmt.Errorf("starting daemon process: %w", err)
	}

	return &Daemon{
		Proc:   proc,
		Stdout: stdout,
		Stderr: stderr,
	}, nil
}

func (n *Node) StartDaemonAndWaitForAPI() (*Daemon, error) {
	daemon, err := n.StartDaemon()
	if err != nil {
		return nil, err
	}
	err = n.WaitOnAPI()
	if err != nil {
		stopErr := daemon.Stop()
		if stopErr != nil {
			n.Log.Warnf("stopping daemon after wait error: %s", stopErr)
		}
		n.Log.Debugf("stdout: %s\n", daemon.Stdout)
		n.Log.Debugf("stderr: %s\n", daemon.Stderr)
		return nil, err
	}
	return daemon, nil
}

func (n *Node) Init() error {
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	err := ProcMust(n.RunKubo(cluster.StartProcRequest{
		Args:   []string{"init"},
		Stdout: stdout,
		Stderr: stderr,
	}))
	if err != nil {
		fmt.Printf("stdout: %s\n", stdout)
		fmt.Printf("stderr: %s\n", stderr)
		return fmt.Errorf("initializing Kubo: %w", err)
	}

	return nil
}

func (n *Node) SetConfig(cfg map[string]string) error {
	// note that this must be serialized since the CLI barfs if it can't acquire the repo lock
	for k, v := range cfg {
		var stdout, stderr = &bytes.Buffer{}, &bytes.Buffer{}
		err := ProcMust(n.RunKubo(cluster.StartProcRequest{
			Args:   []string{"config", "--json", k, v},
			Stdout: stdout,
			Stderr: stderr,
		}))
		if err != nil {
			return fmt.Errorf("setting config %q when initializing: %w", k, err)
		}
	}
	return nil
}

func (n *Node) ConfigureForLocal() error {
	return n.UpdateConfig(func(cfg *config.Config) {
		cfg.Bootstrap = nil
		cfg.Addresses.Swarm = []string{"/ip4/127.0.0.1/tcp/0"}
		cfg.Addresses.API = []string{"/ip4/127.0.0.1/tcp/0"}
		cfg.Addresses.Gateway = []string{"/ip4/127.0.0.1/tcp/0"}
		cfg.Swarm.DisableNatPortMap = true
		cfg.Discovery.MDNS.Enabled = false
	})
}

func (n *Node) ConfigureForRemote() error {
	return n.UpdateConfig(func(cfg *config.Config) {
		cfg.Addresses.Swarm = []string{"/ip4/0.0.0.0/tcp/4001"}
		cfg.Addresses.API = []string{"/ip4/127.0.0.1/tcp/5001"}
		cfg.Addresses.Gateway = []string{"/ip4/127.0.0.1/tcp/8081"}
	})
}

// func (n *Node) ConfigureForLocal(ctx context.Context) error {
// 	return n.SetConfig(ctx, map[string]string{
// 		"Bootstrap":               "[]",
// 		"Addresses.Swarm":         `["/ip4/127.0.0.1/tcp/0"]`,
// 		"Addresses.API":           `["/ip4/127.0.0.1/tcp/0"]`,
// 		"Addresses.Gateway":       `["/ip4/127.0.0.1/tcp/0"]`,
// 		"Swarm.DisableNatPortMap": "true",
// 		"Discovery.MDNS.Enabled":  "false",
// 	})
// }

// func (n *Node) ConfigureForRemote(ctx context.Context) error {
// 	return n.SetConfig(ctx, map[string]string{
// 		"Bootstrap":               "[]",
// 		"Addresses.Swarm":         `["/ip4/0.0.0.0/tcp/0"]`,
// 		"Addresses.API":           `["/ip4/127.0.0.1/tcp/0"]`,
// 		"Addresses.Gateway":       `["/ip4/127.0.0.1/tcp/0"]`,
// 		"Swarm.DisableNatPortMap": "true",
// 		"Discovery.MDNS.Enabled":  "false",
// 	})
// }

func (n *Node) IPFSPath() string {
	return filepath.Join(n.RootDir(), ".ipfs")
}

func (n *Node) APIAddr() (multiaddr.Multiaddr, error) {
	if n.apiAddr != nil {
		return n.apiAddr, nil
	}
	rc, err := n.ReadFile(filepath.Join(n.IPFSPath(), "api"))
	if err != nil {
		return nil, fmt.Errorf("opening api file: %w", err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading api file: %w", err)
	}
	ma, err := multiaddr.NewMultiaddr(string(b))
	if err != nil {
		return nil, err
	}
	n.apiAddr = ma
	return ma, nil
}

func (n *Node) StartKubo(req cluster.StartProcRequest) (*basic.Process, error) {
	req.Env = append(req.Env, "IPFS_PATH="+n.IPFSPath())
	req.Command = n.IPFSBin()

	return n.StartProc(req)
}

func (n *Node) RunKubo(req cluster.StartProcRequest) (*cluster.ProcessResult, error) {
	req.Env = append(req.Env, "IPFS_PATH="+n.IPFSPath())
	req.Command = n.IPFSBin()
	return n.Run(req)
}

func ProcMust(res *cluster.ProcessResult, err error) error {
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("non-zero exit code %d", res.ExitCode)
	}
	return nil
}

// RPCHTTPClient returns an HTTP RPC client configured for this node (https://github.com/ipfs/go-ipfs-http-client)
// We call this the "HTTP Client" to distinguish it from the "API Client". Both are very similar, but slightly different.
func (n *Node) RPCHTTPClient() (*httpapi.HttpApi, error) {
	if n.rcpHTTPClient != nil {
		return n.rcpHTTPClient, nil
	}
	apiMA, err := n.APIAddr()
	if err != nil {
		return nil, fmt.Errorf("getting API address: %w", err)
	}
	c, err := httpapi.NewApiWithClient(apiMA, n.HTTPClient)
	if err != nil {
		return nil, err
	}
	n.rcpHTTPClient = c
	return c, nil
}

// RPCClient returns an RPC client configured for this node (https://github.com/ipfs/go-ipfs-api)
// We call this the "API Client" to distinguish it from the "HTTP Client". Both are very similar, but slightly different.
func (n *Node) RPCAPIClient() (*shell.Shell, error) {
	if n.rpcAPIClient != nil {
		return n.rpcAPIClient, nil
	}
	apiMA, err := n.APIAddr()
	if err != nil {
		return nil, fmt.Errorf("getting API address: %w", err)
	}
	ipAddr, err := apiMA.ValueForProtocol(multiaddr.P_IP4)
	if err != nil {
		return nil, fmt.Errorf("getting ipv4 address: %w", err)
	}
	port, err := apiMA.ValueForProtocol(multiaddr.P_TCP)
	if err != nil {
		return nil, fmt.Errorf("getting TCP port; %w", err)
	}
	u := fmt.Sprintf("%s:%s", ipAddr, port)
	c, err := shell.NewShellWithClient(u, n.HTTPClient), nil
	if err != nil {
		return nil, err
	}
	n.rpcAPIClient = c
	return c, nil
}

func (n *Node) AddrInfo() (*peer.AddrInfo, error) {
	sh, err := n.RPCAPIClient()
	if err != nil {
		return nil, fmt.Errorf("building API client: %w", err)
	}
	idOutput, err := sh.ID()
	if err != nil {
		return nil, fmt.Errorf("fetching id: %w", err)
	}
	peerID, err := peer.Decode(idOutput.ID)
	if err != nil {
		return nil, fmt.Errorf("decoding peer ID: %w", err)
	}
	var multiaddrs []multiaddr.Multiaddr
	for _, a := range idOutput.Addresses {
		ma, err := multiaddr.NewMultiaddr(a)
		if err != nil {
			return nil, fmt.Errorf("decoding multiaddr %q: %w", a, err)
		}
		multiaddrs = append(multiaddrs, ma)
	}
	return &peer.AddrInfo{
		ID:    peerID,
		Addrs: multiaddrs,
	}, nil
}

func (n *Node) RemoteAddrInfo() (*peer.AddrInfo, error) {
	addrInfo, err := n.AddrInfo()
	if err != nil {
		return nil, err
	}
	err = RemoveLocalAddrs(addrInfo)
	if err != nil {
		return nil, err
	}
	return addrInfo, nil
}

func (n *Node) GatewayURL() (*url.URL, error) {
	if n.gatewayAddr != nil {
		return n.gatewayAddr, nil
	}
	rc, err := n.ReadFile(filepath.Join(n.IPFSPath(), "gateway"))
	if err != nil {
		return nil, fmt.Errorf("opening gateway file: %w", err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading gateway file: %w", err)
	}
	u, err := url.Parse(strings.TrimSpace(string(b)))
	if err != nil {
		return nil, fmt.Errorf("parsing gateway url: %w", err)
	}
	n.gatewayAddr = u
	return u, nil
}

func (n *Node) WaitOnAPI() error {
	n.Log.Debug("waiting on API")
	for i := 0; i < 500; i++ {
		if n.checkAPI() {
			n.Log.Debugf("daemon API found")
			n.APIAvailableSince = time.Now()
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	n.Log.Debug("node %s failed to come online", n)
	return errors.New("timed out waiting on API")
}

func (n *Node) checkGateway() bool {
	u, err := n.GatewayURL()
	if err != nil {
		n.Log.Debugf("Gateway not ready: %s", err.Error())
		return false
	}

	c, err := net.Dial("tcp", u.Host)
	if err != nil {
		n.Log.Debugf("could not connect to gateway: %s", err.Error())
		return false
	}

	_ = c.Close()

	return true
}

func (n *Node) checkAPI() bool {
	host, port, err := n.APIHostPort()
	if err != nil {
		n.Log.Debugf("API not ready: %s", err.Error())
		return false
	}

	url := fmt.Sprintf("http://%s:%s/api/v0/id", host, port)

	n.Log.Debugf("checking API at %s", url)

	httpResp, err := n.HTTPClient.Post(url, "", nil)
	if err != nil {
		n.Log.Debugf("API check error: %s", err.Error())
		return false
	}
	defer httpResp.Body.Close()
	resp := struct {
		ID string
	}{}

	respBytes, err := io.ReadAll(httpResp.Body)
	if err != nil {
		n.Log.Debugf("error reading API check response: %s", err.Error())
		return false
	}
	n.Log.Debugf("got API check response: %s", string(respBytes))

	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		n.Log.Debugf("error decoding API check response: %s", err.Error())
		return false
	}
	if resp.ID == "" {
		n.Log.Debugf("API check response for did not contain a Peer ID")
		return false
	}
	n.Log.Debug("API check successful")
	return true
}

func (n *Node) WaitOnGateway() error {
	n.Log.Debug("waiting on gateway")
	for i := 0; i < 500; i++ {
		if n.checkGateway() {
			n.Log.Debugf("daemon gateway found")
			n.GatewayAvailableSince = time.Now()
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	n.Log.Debug("node %s failed to come online", n)
	return errors.New("timed out waiting on gateway")
}

func (n *Node) WaitOnRefreshedRoutingTable() error {
	n.Log.Debug("waiting on refreshed routing table")
	for i := 0; i < 900; i++ { // routing table refresh can take up to 10m - so give some buffer
		if n.checkRefreshedRoutingTable() {
			n.Log.Debugf("routing table is refreshed")
			return nil
		}

		time.Sleep(5 * time.Second)
	}
	n.Log.Debug("node %s failed to refresh its routing table in time", n)
	return errors.New("timed out waiting on routing table refresh")
}

func (n *Node) checkRefreshedRoutingTable() bool {
	host, port, err := n.APIHostPort()
	if err != nil {
		n.Log.Debugf("API not ready: %s", err.Error())
		return false
	}

	// https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-stats-dht
	url := fmt.Sprintf("http://%s:%s/api/v0/stats/dht?arg=wan", host, port)

	n.Log.Debugf("checking refreshed routing table at %s", url)

	httpResp, err := n.HTTPClient.Post(url, "", nil)
	if err != nil {
		n.Log.Debugf("API check error: %s", err.Error())
		return false
	}
	defer httpResp.Body.Close()
	resp := struct {
		Buckets []struct {
			LastRefresh string
		}
	}{}

	respBytes, err := io.ReadAll(httpResp.Body)
	if err != nil {
		n.Log.Debugf("error reading routing table refresh check response: %s", err.Error())
		return false
	}
	n.Log.Debugf("got routing table response")

	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		n.Log.Debugf("error decoding API check response: %s", err.Error())
		return false
	}

	if len(resp.Buckets) == 0 {
		n.Log.Debugf("unexpected bucket length 0")
		return false
	}

	refreshedBuckets := 0
	for _, bucket := range resp.Buckets {
		if bucket.LastRefresh != "" {
			refreshedBuckets += 1
		}
	}

	n.Log.Debugf("refreshed %d of %d buckets", refreshedBuckets, len(resp.Buckets))
	return refreshedBuckets >= 16 || refreshedBuckets == len(resp.Buckets)
}

func (n *Node) APIHostPort() (string, string, error) {
	apiAddr, err := n.APIAddr()
	if err != nil {
		return "", "", err
	}

	ip, err := apiAddr.ValueForProtocol(multiaddr.P_IP4)
	if err != nil {
		return "", "", err
	}

	port, err := apiAddr.ValueForProtocol(multiaddr.P_TCP)
	if err != nil {
		return "", "", err
	}

	return ip, port, nil
}

func (n *Node) ReadConfig() (*config.Config, error) {
	rc, err := n.ReadFile(filepath.Join(n.IPFSPath(), "config"))
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	var cfg config.Config
	err = json.NewDecoder(rc).Decode(&cfg)
	return &cfg, err
}

func (n *Node) WriteConfig(c *config.Config) error {
	b, err := config.Marshal(c)
	if err != nil {
		return err
	}
	err = n.SendFile(filepath.Join(n.IPFSPath(), "config"), bytes.NewReader(b))
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) UpdateConfig(f func(cfg *config.Config)) error {
	cfg, err := n.ReadConfig()
	if err != nil {
		return err
	}
	f(cfg)
	return n.WriteConfig(cfg)
}

func MultiaddrContains(ma multiaddr.Multiaddr, component *multiaddr.Component) (bool, error) {
	v, err := ma.ValueForProtocol(component.Protocol().Code)
	if err == multiaddr.ErrProtocolNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return v == component.Value(), nil
}

func IsLoopback(ma multiaddr.Multiaddr) (bool, error) {
	ip4Loopback, err := multiaddr.NewComponent("ip4", "127.0.0.1")
	if err != nil {
		return false, err
	}
	ip6Loopback, err := multiaddr.NewComponent("ip6", "::1")
	if err != nil {
		return false, err
	}
	ip4MapperIP6Loopback, err := multiaddr.NewComponent("ip6", "::ffff:127.0.0.1")
	if err != nil {
		return false, err
	}
	loopbackComponents := []*multiaddr.Component{ip4Loopback, ip6Loopback, ip4MapperIP6Loopback}
	for _, lc := range loopbackComponents {
		contains, err := MultiaddrContains(ma, lc)
		if err != nil {
			return false, err
		}
		if contains {
			return true, nil
		}
	}
	return false, nil
}

func RemoveLocalAddrs(ai *peer.AddrInfo) error {
	var newMAs []multiaddr.Multiaddr
	for _, addr := range ai.Addrs {
		isLoopback, err := IsLoopback(addr)
		if err != nil {
			return err
		}
		if !isLoopback {
			newMAs = append(newMAs, addr)
		}
	}
	ai.Addrs = newMAs
	return nil
}
