package kubo

import (
	"context"

	"github.com/guseggert/clustertest/cluster/basic"
	"go.uber.org/zap"
)

var defaultLogger *zap.SugaredLogger

const defaultKuboVersion = "latest-release"

func init() {
	l, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defaultLogger = l.Sugar()
}

func New(bc *basic.Cluster) *Cluster {
	return &Cluster{
		Cluster: bc,
		Ctx:     context.Background(),
	}
}

func Must(err error) {
	if err != nil {
		panic(err)
	}
}

func Must2[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}

func (c *Cluster) Context(ctx context.Context) *Cluster {
	newC := *c
	newC.Ctx = ctx
	newC.Cluster = newC.Cluster.Context(ctx)
	return &newC
}

type Cluster struct {
	*basic.Cluster
	Ctx context.Context
}

func (c *Cluster) NewNodes(n int) ([]*Node, error) {
	var kuboNodes []*Node
	nodes, err := c.Cluster.NewNodes(n)
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		kn := newNode(n).WithNodeLogger(c.Log).Context(c.Ctx)
		kuboNodes = append(kuboNodes, kn)
	}
	return kuboNodes, nil
}

func (c *Cluster) MustNewNodes(n int) []*Node {
	return Must2(c.NewNodes(n))
}
