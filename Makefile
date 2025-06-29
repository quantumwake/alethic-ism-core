# Makefile
.PHONY: build push deploy all version

# Default image name - can be overridden with make IMAGE=your-image-name
IMAGE ?= krasaee/alethic-ism-core:latest

# Ensure scripts are executable
.PHONY: init
init:
	chmod +x docker_build.sh

# Build the Docker image
.PHONY: build
build:
	sh docker_build.sh -t $(IMAGE)

# Push the Docker image to registry
.PHONY: push
push:
	docker push $(IMAGE)

# Deploy the application (placeholder - adjust based on your deployment needs)
.PHONY: deploy
deploy:
	@echo "Deploy target not implemented yet"
	@echo "Add deployment logic here when needed"

# Version bump (patch version)
version:
	@echo "Bumping patch version..."
	@git fetch --tags
	@LATEST_TAG=$$(git describe --tags --abbrev=0 2>/dev/null || echo ""); \
	if [[ -z "$$LATEST_TAG" ]]; then \
		MAJOR=0; MINOR=1; PATCH=0; \
		OLD_TAG="<none>"; \
	else \
		OLD_TAG="$$LATEST_TAG"; \
		VERSION="$${LATEST_TAG#v}"; \
		IFS='.' read -r MAJOR MINOR PATCH <<< "$$VERSION"; \
		PATCH=$$((PATCH + 1)); \
	fi; \
	NEW_TAG="v$${MAJOR}.$${MINOR}.$${PATCH}"; \
	git tag -a "$$NEW_TAG" -m "Release $$NEW_TAG"; \
	git push origin "$$NEW_TAG"; \
	echo "œ bumped $${OLD_TAG} ’ $${NEW_TAG}"

# Build and push
.PHONY: all
all: build push

# Clean up old images and containers (optional)
.PHONY: clean
clean:
	@docker system prune -f