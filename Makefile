nodeagent:
#	GOBIN="$(PWD)" go install github.com/guseggert/clustertest/cmd/agent@latest
#	GOBIN="$(PWD)" go install ../clustertest/cmd/agent
	go build -o agent ~/git/clustertest/cmd/agent/main.go
	mv agent nodeagent
.PHONY: nodeagent
