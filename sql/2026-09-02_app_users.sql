-- Premium dashboard users table — email/password auth, auto-upgrade on signup.
-- Runs in scraper schema (same DB as gmaps_listings/emails/linkedin).

BEGIN;

CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE IF NOT EXISTS scraper.app_users (
  id            BIGSERIAL PRIMARY KEY,
  email         CITEXT UNIQUE NOT NULL,
  password_hash TEXT        NOT NULL,
  entitlement   JSONB       NOT NULL DEFAULT '{"tier":"pro","rows_limit":null}'::jsonb,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at TIMESTAMPTZ,
  searches_run  INTEGER     NOT NULL DEFAULT 0,
  rows_exported INTEGER     NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS app_users_email_idx ON scraper.app_users(email);

COMMIT;
