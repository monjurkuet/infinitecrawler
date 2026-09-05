# InfiniteCrawler Admin Ops Panel (web-admin)

Browser-based ops dashboard for the InfiniteCrawler pipeline. Vite + React
SPA, talks only to the internal API (`:8015`) with a session-stored Bearer
token (`sessionStorage` — cleared when the tab closes).

- Dev server: `pnpm dev` → http://127.0.0.1:5174
- Built output: `pnpm build` → `dist/`

## What it shows

- **Overview** — live counts (search results, listings, queue depths,
  emails) with 1h/6h/24h deltas
- **Daemons** — systemd unit state, restart counters, last-flush heartbeat
- **Queues** — Redis queue lengths (`gmaps:pending`/`processing`/`completed`,
  per-prefix)
- **Logs** — tail of every IC log in `/var/log/infinitecrawler/`

## Auth

`Login.tsx` collects the `INFINITECRAWLER_API_TOKEN` (the internal API's
Bearer secret), stores it in `sessionStorage` under `ic_admin_token`, and
issues every request through `api.ts::request()` (adds
`Authorization: Bearer <token>`, redirects to `/login` on a 401).

The token NEVER reaches the companies-browser `web/` SPA or the premium API
(`:8016`) — those use a different auth scheme.

## Systemd unit

`infinitecrawler-web-admin.service` (installed under
`~/.config/systemd/user/`):

- `ExecStart=/usr/bin/pnpm dev --host 127.0.0.1` in `WorkingDirectory=.../web-admin`
- `MemoryMax=1G`, `Restart=on-failure`, `RestartSec=10`
- RequiresMountsFor=/run/media/growloop (external-drive safe)
- `WantedBy=default.target` + `loginctl enable-linger` — auto-starts on boot

Logs: `/var/log/infinitecrawler/infinitecrawler-admin-web.log`.

## Stack notes

- React 19 + TypeScript, Vite 8, react-router. The folder came from a
  `pnpm create vite` scaffold, so the default Vite `react-ts` template files
  (favicon, `assets/`) are still present but unused.
- `pnpm` is the package manager here (lockfile: `web-admin/pnpm-lock.yaml`).
  Don't switch to `npm` — the systemd unit assumes `pnpm`.
