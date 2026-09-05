# Premium Dashboard (paywalled lead access)

Self-serve email/password dashboard exposing the enriched lead database
(Google Maps listings + emails + LinkedIn) to end users. Runs side-by-side
with the internal API — separate process, separate port, separate auth.

- Premium API: FastAPI on :8016 (`api_premium/`), JWT auth (bcrypt + PyJWT).
- Frontend: Vite + React + Tailwind SPA on :5173 (`web/`), proxies
  `/auth/*` and `/premium/*` to :8016 in dev.
- Postgres-only identity: `scraper.app_users` + `scraper.auth_attempts`.

## Signed-up flow

1. `POST /auth/register` → bcrypt-hashed user row, entitlement
   `{"tier":"pro","rows_limit":null}` (unlimited), JWT returned.
2. `POST /auth/login` → JWT (24h HS256, `JWT_SECRET` env).
3. SPA stores the JWT in localStorage and sends `Authorization: Bearer`
   on every call.

Signups auto-upgrade — there is no billing gate yet (phase 1). To demote a
user manually:

```sql
UPDATE scraper.app_users
SET entitlement = '{"tier":"free","rows_limit":200}'::jsonb
WHERE email = '...';
```

## Auth endpoints

| Method | Path | Public | Notes |
|---|---|---|---|
| POST | /auth/register | yes | 201, 409 on duplicate |
| POST | /auth/login | yes | 401 bad creds, 429 after 5 fails / 10 min |
| GET | /auth/me | no | full user + entitlement |
| POST | /auth/refresh | no | fresh 24h token |
| POST | /auth/change-password | no | `{current_password, new_password}` |

Rate limiting lives in `scraper.auth_attempts` — every login attempt is
logged with IP + success flag; >5 failures in 10 minutes → 429.

## Premium endpoints

All under `/premium/*`, JWT required, unlimited rows on `pro`:

- `GET /premium/leads?city=&category=&min_rating=&has_email=&q=&page=&size=`
  — paginated list (max 200/page), joins emails + LinkedIn inline.
- `GET /premium/leads/{id}` — full listing row + payload + social_links +
  `emails_full[]` + `linkedin_profiles[]`.
- `GET /premium/export.csv?<same filters>` — full-row CSV (17 columns),
  streamed, increments `rows_exported`.
- `GET /premium/stats` — inventory totals + per-user usage.

## Deployment (this host)

- Premium API (`api_premium/`): FastAPI on `:8016`, JWT auth (`/auth/*`),
  paywalled `/premium/*`, `JWT_SECRET` from `./.jwt_secret` (0600).
- Subscriber SPA (`web/`): Vite + React + Tailwind on `:5173`, proxies
  `/auth/*` + `/premium/*` → :8016 in dev.
- Admin ops SPA (`web-admin/`): Vite + React on `:5174` (see `README.md` →
  admin), logs into the internal `:8015` API with the `INFINITECRAWLER_API_TOKEN` Bearer.

`systemd/` (installed under `~/.config/systemd/user/`, all `enabled`,
external-drive-safe via `RequiresMountsFor=/run/media/growloop/`):

- `infinitecrawler-premium-api.service` — `uvicorn` + env from `./.env`, `MemoryMax=500M`
- `infinitecrawler-web.service` — `pnpm dev` for the subscriber SPA
- `infinitecrawler-web-admin.service` — `pnpm dev` for the ops SPA

With `loginctl enable-linger` (already set on this host) all three start at
boot and auto-restart on failure. For production serve `web/{dist}` (and
`web-admin/dist`) statically via Nginx/Caddy, proxying `/auth`+`/premium` to
:8016 and `/admin/*` to :8015.

Logs: `/var/log/infinitecrawler/infinitecrawler-premium-api.log` and
`infinitecrawler-web.log`.

## Data migrations

Applied via `sql/` (manual `psql -f`):
- `2026-09-02_app_users.sql` (users + citext ext)
- `2026-09-02_auth_attempts.sql` (login rate-limit log)

## Dev runbook

```bash
cd /run/media/growloop/codebase/infinitecrawler
# backend
/home/growloop/.venvs/ic/bin/python3 -m api_premium.main   # needs .env + JWT_SECRET
# frontend
cd web && pnpm install && pnpm dev
```

Vite dev proxies `/auth` & `/premium` → `http://127.0.0.1:8016` (see
`web/vite.config.ts`).
