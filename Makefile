REPO_SERVER=019120760881.dkr.ecr.us-east-1.amazonaws.com

docker-push:
	$(eval GIT_TAG := $(shell git rev-parse --short HEAD))
	docker build -f Dockerfile_latency -t "${REPO_SERVER}/probelab:tiros-${GIT_TAG}" .
	docker push "${REPO_SERVER}/probelab:tiros-${GIT_TAG}"

nodeagent:
	GOARCH=amd64 GOOS=linux GOBIN="$(PWD)" go install github.com/guseggert/clustertest/cmd/agent@latest
	mv agent nodeagent

.PHONY: nodeagent
