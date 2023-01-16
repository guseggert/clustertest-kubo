nodeagent:
	GOBIN="$(PWD)" go install github.com/guseggert/clustertest/cmd/agent@latest
	mv agent nodeagent
.PHONY: nodeagent
