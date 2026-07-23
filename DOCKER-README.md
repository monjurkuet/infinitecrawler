# InfiniteCrawler — Docker Image

[![Docker Image](https://img.shields.io/docker/v/monjurkuet/infinitecrawler?logo=docker)](https://hub.docker.com/r/monjurkuet/infinitecrawler)
[![Docker Pulls](https://img.shields.io/docker/pulls/monjurkuet/infinitecrawler)](https://hub.docker.com/r/monjurkuet/infinitecrawler)
[![Image Size](https://img.shields.io/docker/image-size/monjurkuet/infinitecrawler/latest)](https://hub.docker.com/r/monjurkuet/infinitecrawler)

Continuous 24/7 Google Maps scraping pipeline with systemd daemons, REST API, and eternal queue management.

## Quick Start

```bash
# Pull image
docker pull monjurkuet/infinitecrawler:latest

# Run with Redis
docker run -d \
  -e PG_HOST=your-postgres-host \
  -e PG_PASSWORD=your-password \
  -e REDIS_HOST=redis-host \
  -p 8015:8015 \
  monjurkuet/infinitecrawler:latest

# Or with docker-compose
curl -o docker-compose.yml https://raw.githubusercontent.com/yourusername/infinitecrawler/main/docker-compose.yml
docker compose up -d --pull always
```

## Environment Variables

| Variable | Default | Required | Notes |
|----------|---------|----------|-------|
| `PG_HOST` | localhost | ✓ | PostgreSQL hostname |
| `PG_PORT` | 5432 | | PostgreSQL port |
| `PG_USER` | infinitecrawler | | Database user |
| `PG_PASSWORD` | changeme | ✓ | Database password |
| `PG_DB` | infinitecrawler | | Database name |
| `REDIS_HOST` | localhost | | Redis hostname |
| `REDIS_PORT` | 6379 | | Redis port |
| `INFINITECRAWLER_API_HOST` | 0.0.0.0 | | API bind address |
| `INFINITECRAWLER_API_PORT` | 8015 | | API port |
| `INFINITECRAWLER_API_TOKEN` | changeme | ✓ | Bearer token for auth |
| `PINCHTAB_HOST` | host.docker.internal | | Pinchtab server (daemons) |
| `PINCHTAB_PORT` | 9868 | | Pinchtab port |
| `PINCHTAB_TOKEN` | changeme | | Pinchtab auth token |

## Services

### API Server (Default)
```bash
docker run -p 8015:8015 monjurkuet/infinitecrawler:latest
```

Endpoints:
- `GET /` — Root (health check)
- `GET /docs` — Swagger UI
- `GET /openapi.json` — OpenAPI spec
- `GET /leads` — Query leads (requires DB)
- `GET /monitor` — Pipeline stats (requires Redis)

### Daemons (Optional)

Search & listing daemons require **pinchtab** running on the host:

```bash
# With docker-compose
docker compose --profile daemons up -d

# Or manual
docker run -e PINCHTAB_HOST=host.docker.internal \
  -e PINCHTAB_TOKEN=your-token \
  monjurkuet/infinitecrawler:latest \
  python -m daemons.search_daemon
```

## Image Details

- **Base:** `python:3.12-slim`
- **Size:** ~300MB (multi-stage build)
- **Layers:** 9 (optimized caching)
- **Entrypoint:** API server (port 8015)
- **Health Check:** HTTP GET `/` every 30s

## Docker Compose

Full setup with Redis:

```yaml
version: '3.8'
services:
  api:
    image: monjurkuet/infinitecrawler:latest
    environment:
      PG_HOST: your-postgres.com
      PG_PASSWORD: secret
      REDIS_HOST: redis
      INFINITECRAWLER_API_TOKEN: your-token
    ports:
      - "8015:8015"
    depends_on:
      - redis
  
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
```

Start:
```bash
docker compose up -d
curl http://localhost:8015/
```

## Volume Mounting

Mount source code for development:

```bash
docker run -v $(pwd):/app monjurkuet/infinitecrawler:latest \
  python -m daemons.search_daemon
```

## Tags

- `latest` — Latest release
- `1.0.0` — Stable version
- `main` — CI/CD from main branch (if using GitHub Actions)

## Build from Source

```bash
git clone https://github.com/yourusername/infinitecrawler.git
cd infinitecrawler
docker build -t monjurkuet/infinitecrawler:dev .
docker run monjurkuet/infinitecrawler:dev
```

## Troubleshooting

### API won't start
```bash
docker logs <container_id>
# Check: PG_HOST, REDIS_HOST, ports not in use
```

### PostgreSQL connection refused
```bash
docker run -e PG_HOST=host.docker.internal monjurkuet/infinitecrawler:latest
# On Docker Desktop, use host.docker.internal instead of localhost
```

### Redis connection refused
```bash
# Ensure Redis is running and accessible
docker run --net host monjurkuet/infinitecrawler:latest
```

## Performance Tips

1. **Use alpine base** (already in use)
2. **Increase Docker memory:** `docker run -m 2g ...`
3. **Enable Redis persistence:** `--appendonly yes`
4. **Scale daemons horizontally** with docker-compose profiles

## Documentation

- [Full Deployment Guide](https://github.com/yourusername/infinitecrawler/blob/main/DOCKER-DEPLOYMENT.md)
- [Source Repository](https://github.com/yourusername/infinitecrawler)
- [README](https://github.com/yourusername/infinitecrawler/blob/main/README.md)

## License

See [LICENSE](https://github.com/yourusername/infinitecrawler/blob/main/LICENSE) in the repository.

## Support

Issues? Submit to [GitHub Issues](https://github.com/yourusername/infinitecrawler/issues)
