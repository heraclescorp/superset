#!/bin/bash
set -euo pipefail
cd "${0%/*}"

# Usage: ./deploy.sh <immutable-image-tag>. This never pushes latest.
IMAGE_TAG="${1:?Usage: ./deploy.sh <immutable-image-tag>}"
if [[ "$IMAGE_TAG" == "latest" ]]; then
  echo "Use an immutable image tag instead of latest" >&2
  exit 1
fi

REGISTRY="199658938451.dkr.ecr.us-east-2.amazonaws.com"
IMAGE_NAME="$REGISTRY/superset:$IMAGE_TAG"

export DOCKER_CONFIG="$(mktemp -d)"
trap 'rm -rf "$DOCKER_CONFIG"' EXIT

echo 'Logging into AWS ECR'
aws ecr get-login-password --region us-east-2 ${HERACLES_AWS_PROFILE:-} | \
  docker login --username AWS --password-stdin "$REGISTRY"

./build.sh "$IMAGE_TAG"

echo "Pushing image $IMAGE_NAME"
docker push "$IMAGE_NAME"
