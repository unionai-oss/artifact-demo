define IMAGE_BUILD
docker buildx build --ssh default \
	--platform linux/$(1) \
	--tag ghcr.io/unionai/artifacts:sandbox \
	--output type=docker,dest=images/tar/$(1)/artifact-service.tar \
	src

endef


.PHONY: .venv
.venv:
	python -m venv .venv
	source .venv/bin/activate; \
		pip install -U pip && \
		pip install -r requirements.in && \
		pip install --no-deps -U --force-reinstall "git+https://github.com/flyteorg/flyteidl.git@7aa32a59bae939e629cbcf12e85c096233028d38"

.PHONY: test
test:
	source .venv/bin/activate; \
	python -m pytest -s tests

.PHONY: image
image:
	$(foreach arch,amd64 arm64,$(call IMAGE_BUILD,$(arch)))

# Run make image
# then go into flyte2/docker/sandbox-bundled directory and copy
# then cp /Users/ytong/go/src/github.com/unionai/experiments/artifact-service/artifact-service/src/images/tar/arm64/artifact-service.tar images/tar/arm64/
# and also for amd64