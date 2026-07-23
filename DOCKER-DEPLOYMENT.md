# Docker Deployment Guide — InfiniteCrawler

## Quick Start

```bash
# 1. Clone and configure
git clone <repo>
cd infinitecrawler
cp .env.example .env
nano .env  # Set PG_HOST, PG_PASSWORD, PINCHTAB_TOKEN

# 2. Start API + Redis
docker compose up -d --pull always

# 3. Verify
docker compose ps
curl http://localhost:8016/

# 4. View logs
docker compose logs -f api
```

## Images & Artifacts

| Component | Size | Details |
|-----------|------|---------|
| infinitecrawler:latest | ~300MB | Multi-stage build, Python 3.12, all dependencies |
| redis:7-alpine | ~28MB | Queue backend (internal network) |

## Services

### API Server
- **Port:** 8016 (localhost) → 8015 (container)
- **Healthcheck:** HTTP GET `/` every 30s
- **Status:** `docker compose ps api`
- **Logs:** `docker compose logs -f api`
- **Swagger:** http://localhost:8016/docs

### Redis
- **Port:** 6379 (internal, no external exposure)
- **Persistence:** Append-only file in `redis_data` volume
- **Healthcheck:** `redis-cli ping`
- **Commands:** `docker compose exec redis redis-cli`

### Search Daemon (Optional)
- **Profile:** `daemons`
- **Requirements:** pinchtab on host (port 9868)
- **Start:** `docker compose --profile daemons up -d search-daemon`
- **Restart Policy:** `unless-stopped`

### Listing Daemon (Optional)
- **Profile:** `daemons`
- **Requirements:** pinchtab on host (port 9868)
- **Start:** `docker compose --profile daemons up -d listing-daemon`
- **Restart Policy:** `unless-stopped`

## Configuration

### Environment Variables

| Variable | Default | Notes |
|----------|---------|-------|
| `PG_HOST` | localhost | Remote VPS hostname |
| `PG_PORT` | 5432 | PostgreSQL port |
| `PG_USER` | infinitecrawler | Database user |
| `PG_PASSWORD` | changeme | **⚠️ CHANGE THIS** |
| `PG_DB` | infinitecrawler | Database name |
| `REDIS_HOST` | redis | Internal service name |
| `REDIS_PORT` | 6379 | Redis port |
| `PINCHTAB_HOST` | host.docker.internal | Pinchtab server (Docker Desktop) |
| `PINCHTAB_PORT` | 9868 | Pinchtab API port |
| `PINCHTAB_TOKEN` | changeme | **⚠️ SET FROM CONFIG** |
| `INFINITECRAWLER_API_TOKEN` | changeme | **⚠️ CHANGE THIS** |

Edit `.env` or use inline:
```bash
docker compose up -d -e PG_HOST=your-vps.com -e PG_PASSWORD=secret
```

## Common Commands

### View Status
```bash
docker compose ps                    # All services
docker compose ps api                # Single service
docker compose logs -f api           # Live logs
docker compose logs --tail=100 api   # Last 100 lines
```

### Shell Access
```bash
docker compose exec api bash         # Shell into API container
docker compose exec api python -c "import api; print(api.__version__)"
docker compose exec redis redis-cli  # Redis CLI
docker compose exec redis redis-cli KEYS "*"  # List all keys
```

### Restart Services
```bash
docker compose restart api           # Restart API
docker compose restart              # Restart all
docker compose down && docker compose up -d  # Full restart
```

### Scale (for testing)
```bash
docker compose up -d --scale search-daemon=3  # 3 search daemons
# Note: Must remove port mappings from compose first
```

### Stop & Clean
```bash
docker compose down                  # Stop services
docker compose down -v               # Stop + remove volumes (⚠️ data loss)
docker system prune -a              # Remove unused images
```

## Daemon Integration (Pinchtab)

To enable search and listing daemons:

1. **Start pinchtab on host:**
   ```bash
   systemctl --user start pinchtab  # Or your pinchtab service
   ```

2. **Verify connectivity:**
   ```bash
   docker compose exec api curl http://host.docker.internal:9868/health
   ```

3. **Start daemons:**
   ```bash
   docker compose --profile daemons up -d search-daemon listing-daemon
   ```

4. **Monitor:**
   ```bash
   docker compose logs -f search-daemon
   docker compose logs -f listing-daemon
   ```

5. **Restart if needed:**
   ```bash
   docker compose restart search-daemon listing-daemon
   ```

## Registry Push

### Docker Hub
```bash
# Set credentials
export DOCKER_USERNAME=your-username
export DOCKER_PASSWORD=your-token

# Build & Push
docker build -t $DOCKER_USERNAME/infinitecrawler:latest .
docker push $DOCKER_USERNAME/infinitecrawler:latest

# Or use GitHub Actions (set secrets: DOCKER_USERNAME, DOCKER_PASSWORD)
# Automatically builds and pushes on push to `main` branch
```

### Private Registry
```bash
docker build -t your-registry.com/infinitecrawler:latest .
docker login your-registry.com
docker push your-registry.com/infinitecrawler:latest
```

## Health Checks

API has built-in health checks:
- **Interval:** 30 seconds
- **Timeout:** 10 seconds
- **Start Period:** 5 seconds
- **Retries:** 3 before marked unhealthy
- **Endpoint:** `GET /`

Monitor status:
```bash
docker compose ps api  # Shows health in STATUS column
docker inspect $(docker compose ps -q api) | grep -A 5 '"Health"'
```

## Logs

### API Logs
```bash
docker compose logs api
docker compose logs -f api                    # Follow
docker compose logs api --tail=50             # Last 50 lines
docker compose logs api --since=10m           # Last 10 minutes
docker compose logs api --until=5m            # Up to 5 minutes ago
```

### Redis Logs
```bash
docker compose logs redis
docker compose logs -f redis
```

### All Services
```bash
docker compose logs
docker compose logs -f
```

## Troubleshooting

### API won't start
```bash
# Check logs
docker compose logs api

# Common issues:
# - PostgreSQL unreachable: Set PG_HOST correctly
# - Port 8016 in use: Change port in docker-compose.yml
# - Out of memory: Increase Docker memory limit
```

### Redis connection issues
```bash
# Test connection
docker compose exec redis redis-cli ping

# Check persistence
docker compose exec redis redis-cli DBSIZE
docker compose exec redis redis-cli INFO persistence
```

### Daemon won't connect to pinchtab
```bash
# Check from container
docker compose exec search-daemon curl http://host.docker.internal:9868/health

# Verify on host
curl -H "Authorization: Bearer $PINCHTAB_TOKEN" http://127.0.0.1:9868/health
```

### Full reset
```bash
docker compose down -v                  # Stop + remove volumes
docker system prune -a --volumes        # Clean all Docker
docker compose up -d --pull always      # Fresh start
```

## Performance Tuning

### Memory
```bash
# Increase Docker memory in Docker Desktop settings
# Or via docker daemon:
docker update --memory 4g <container_id>
```

### Redis Persistence
In `docker-compose.yml`:
```yaml
redis:
  command: redis-server --appendonly yes --save 60 1000
```

### Python GC (for daemons)
```bash
docker compose exec search-daemon python -c "import gc; print(gc.collect())"
```

## Development

### Local testing
```bash
docker compose build --no-cache infinitecrawler-api
docker compose up -d api
docker compose logs -f api
```

### Run tests in container
```bash
docker compose exec api python -m pytest tests/ -v --cov
```

### Interactive shell
```bash
docker compose run --rm api bash
```

## Deployment Checklist

- [ ] `.env` file created with real credentials
- [ ] PostgreSQL reachable from container
- [ ] Redis running and healthy
- [ ] API responding on http://localhost:8016/
- [ ] (Optional) Pinchtab running and accessible
- [ ] (Optional) Daemons started with `--profile daemons`
- [ ] Docker image built and tagged
- [ ] Image pushed to registry (if using registry)
- [ ] Logs monitored for errors
- [ ] Health checks passing

## References

- [Docker Compose docs](https://docs.docker.com/compose/)
- [Multi-stage builds](https://docs.docker.com/build/building/multi-stage/)
- [Health checks](https://docs.docker.com/compose/compose-file/compose-file-v3/#healthcheck)
- [Profiles](https://docs.docker.com/compose/compose-file/compose-file-v3/#profiles)
