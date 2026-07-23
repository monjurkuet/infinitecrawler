#!/bin/bash
# Push InfiniteCrawler image to Docker Hub or custom registry

set -e

REGISTRY="${1:-docker.io}"
USERNAME="${2:-infinitecrawler}"
TAG="${3:-latest}"
IMAGE_NAME="${REGISTRY}/${USERNAME}/infinitecrawler:${TAG}"

echo "=== Docker Image Push ==="
echo "Image: $IMAGE_NAME"
echo

# Build image
echo "Building image..."
docker build -t "$IMAGE_NAME" -f Dockerfile .
echo "✓ Built: $IMAGE_NAME"
echo

# Login (if Docker Hub)
if [[ "$REGISTRY" == "docker.io" ]]; then
    echo "Docker Hub login required. Use your username and access token."
    docker login
    echo
fi

# Push
echo "Pushing image..."
docker push "$IMAGE_NAME"
echo "✓ Pushed: $IMAGE_NAME"
echo

# Show info
echo "=== Image pushed successfully ==="
echo
echo "Pull with:"
echo "  docker pull $IMAGE_NAME"
echo
echo "Run with:"
echo "  docker run -e PG_HOST=your-host -e REDIS_HOST=redis -p 8015:8015 $IMAGE_NAME"
