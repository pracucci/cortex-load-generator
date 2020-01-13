GIT_BRANCH = $(shell git rev-parse --abbrev-ref HEAD)
GIT_COMMIT = $(shell git rev-parse --short HEAD)

build-image:
	docker build -t cortex-load-generator .

publish-image: build-image
	docker tag cortex-load-generator:latest pracucci/cortex-load-generator:$(GIT_BRANCH)-$(GIT_COMMIT)
	docker push pracucci/cortex-load-generator:$(GIT_BRANCH)-$(GIT_COMMIT)
