
.PHONY: image
image: VER=$(shell git log -1 --pretty=format:"%H")
image:
	docker buildx build --platform linux/arm64,linux/amd64 -f Dockerfile.artifact --push -t ghcr.io/flyteorg/flytecookbook:a_$(VER) artifact_ux
