-- Premium Dashboard — hardening pass (rate limit, password change, refresh)
-- Runs against the same infinitecrawler DB; safe to re-run (idempotent).

BEGIN;

-- Rate-limit/login-attempt guard
CREATE TABLE IF NOT EXISTS scraper.auth_attempts (
  id           BIGSERIAL PRIMARY KEY,
  email        CITEXT NOT NULL,
  ip           INET,
  success      BOOLEAN NOT NULL,
  attempted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS auth_attempts_email_idx ON scraper.auth_attempts(email, attempted_at);
CREATE INDEX IF NOT EXISTS auth_attempts_time_idx  ON scraper.auth_attempts(attempted_at DESC);

COMMIT;
