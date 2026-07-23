#!/bin/bash
# Quick setup script for InfiniteCrawler Docker deployment

set -e

echo "=== InfiniteCrawler Docker Setup ==="
echo

# Check if .env exists
if [ ! -f .env ]; then
    echo "⚠️  .env file not found. Creating from .env.example..."
    cp .env.example .env
    echo "✓ Created .env — edit it with your configuration"
    echo
fi

# Show current setup
echo "Current Configuration:"
echo "  PG_HOST: ${PG_HOST:-localhost}"
echo "  PG_DB: ${PG_DB:-infinitecrawler}"
echo "  REDIS_HOST: redis (internal)"
echo "  API_PORT: 8016 (localhost)"
echo

# Build image
echo "Building Docker image..."
docker build -t infinitecrawler:latest -f Dockerfile .
echo "✓ Image built: infinitecrawler:latest"
echo

# Start services
echo "Starting services..."
docker compose up -d --pull always
echo "✓ Services started"
echo

# Wait for healthchecks
echo "Waiting for services to be healthy..."
sleep 5
docker compose ps
echo

# Test API
echo "Testing API..."
docker compose exec api curl -s http://localhost:8015/ | python -m json.tool
echo

# Show next steps
echo "=== Next Steps ==="
echo
echo "1. Configure environment:"
echo "   edit .env"
echo
echo "2. Start API only (default):"
echo "   docker compose up -d"
echo
echo "3. Start with daemons (requires pinchtab):"
echo "   docker compose --profile daemons up -d"
echo
echo "4. View logs:"
echo "   docker compose logs -f api"
echo "   docker compose logs -f search-daemon"
echo
echo "5. Stop services:"
echo "   docker compose down"
echo
echo "6. Push to registry:"
echo "   docker build -t your-registry/infinitecrawler:latest ."
echo "   docker push your-registry/infinitecrawler:latest"
echo
echo "API available at: http://localhost:8016"
echo "Swagger docs: http://localhost:8016/docs"
