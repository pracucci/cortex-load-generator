GIT_BRANCH = $(shell git rev-parse --abbrev-ref HEAD)
GIT_COMMIT = $(shell git rev-parse --short HEAD)

build-image:
	docker build -t cortex-load-generator .

publish-image: build-image
	docker tag cortex-load-generator:latest pracucci/cortex-load-generator:$(GIT_BRANCH)-$(GIT_COMMIT)
	docker push pracucci/cortex-load-generator:$(GIT_BRANCH)-$(GIT_COMMIT)

run-proxy:
	go run ./tools/proxy/

test:
	go test ./...

lint:
	docker run -t --rm -v $(shell pwd):/code -v cortex-load-generator-lint-cache:/root/.cache -w /code golangci/golangci-lint:v1.53.3 golangci-lint run ./...
