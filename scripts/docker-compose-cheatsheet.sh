#!/bin/bash
# Quick reference for common docker-compose commands

cat << 'EOF'
=== InfiniteCrawler Docker-Compose Commands ===

🚀 START & STOP
  docker compose up -d              # Start API + Redis
  docker compose --profile daemons up -d  # Start with search & listing daemons
  docker compose down               # Stop all services
  docker compose down -v            # Stop & remove volumes (⚠️ data loss)

📊 MONITORING
  docker compose ps                 # Show running services
  docker compose logs api           # View API logs
  docker compose logs -f redis      # Follow Redis logs (Ctrl+C to exit)
  docker compose logs --tail=50 search-daemon  # Last 50 lines of search-daemon

⚙️ CONFIGURATION
  cat .env.example                  # View example config
  nano .env                         # Edit configuration
  docker compose config             # Show resolved compose config

🔧 MANAGEMENT
  docker compose exec api bash      # Shell into API container
  docker compose exec redis redis-cli ping  # Test Redis connection
  docker compose restart api        # Restart API container
  docker compose pull               # Pull latest images
  docker compose build              # Rebuild images

📈 SCALING
  docker compose up -d --scale search-daemon=2  # Run 2 search daemons (requires removed ports)

🐛 DEBUGGING
  docker compose exec api python -m pytest tests/ -v  # Run tests in container
  docker compose exec api curl http://localhost:8015/docs  # Access Swagger UI
  docker compose events            # Watch container events

🧹 CLEANUP
  docker compose down               # Stop services
  docker system prune              # Remove unused images/volumes
  docker compose down -v           # Remove everything (including data!)

📝 API ENDPOINTS (if running)
  http://localhost:8016/           # Root
  http://localhost:8016/docs        # Swagger UI
  http://localhost:8016/openapi.json # OpenAPI spec

⚡ QUICK TEST
  docker compose exec api curl http://localhost:8015/ -s | python -m json.tool

EOF
