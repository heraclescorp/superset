#!/bin/bash
set -euo pipefail
cd "${0%/*}"

# Production builds require a versioned tag so unrelated builds cannot replace latest.
IMAGE_TAG="${1:?Usage: ./build.sh <immutable-image-tag>}"
if [[ "$IMAGE_TAG" == "latest" ]]; then
  echo "Use an immutable image tag instead of latest" >&2
  exit 1
fi

IMAGE_NAME="199658938451.dkr.ecr.us-east-2.amazonaws.com/superset:$IMAGE_TAG"
GIT_SHA="$(git rev-parse HEAD)"

# Build the full fork on the pinned batteries-included runtime, including Chromium.
# Host networking can be selected on builders whose bridge cannot reach registries.

echo "Building image $IMAGE_NAME"
docker build --platform=linux/amd64 --target superset \
  --network "${DOCKER_BUILD_NETWORK:-default}" \
  --build-arg "SUPERSET_GIT_SHA=$GIT_SHA" \
  -t "$IMAGE_NAME" -f ./aven.Dockerfile .
