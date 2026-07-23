#!/bin/bash
# Health check and monitoring for docker-compose services

set -e

INTERVAL="${1:-10}"
MAX_RETRIES="${2:-30}"

echo "=== Service Health Monitor ==="
echo "Checking every ${INTERVAL}s (max $MAX_RETRIES attempts)..."
echo

check_service() {
    local service=$1
    local max=$2
    local count=0

    while [ $count -lt $max ]; do
        if docker compose ps "$service" --format "json" | jq -e '.[0].State == "running"' &>/dev/null; then
            echo "✓ $service is running"
            return 0
        fi
        count=$((count + 1))
        if [ $count -lt $max ]; then
            sleep "$INTERVAL"
        fi
    done

    echo "✗ $service failed to start after $((max * INTERVAL))s"
    return 1
}

# Check services
echo "Checking API..."
check_service "api" "$MAX_RETRIES"

echo "Checking Redis..."
check_service "redis" "$MAX_RETRIES"

echo
echo "=== All services healthy ==="
echo
docker compose ps
