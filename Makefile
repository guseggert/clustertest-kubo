nodeagent:
	GOARCH=amd64 GOOS=linux GOBIN="$(PWD)" go install github.com/guseggert/clustertest/cmd/agent@latest
	mv agent nodeagent

.PHONY: nodeagent
