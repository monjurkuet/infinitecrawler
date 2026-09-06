"""Shared website URL normalization for all scraper write paths.

Every daemon that persists a business website (browser listing daemon,
Places API daemon, nearby scanner, BBB scraper, websites sync job) must
pass the raw value through `normalize_website` first so the DB never
receives scheme-less, whitespace-padded, or phone-number-as-URL garbage.

Social-profile links (a business whose only "website" is its Facebook
page) are NOT business websites: `is_social_url` detects them so callers
can route them to `social_links` instead of the `website` column.
"""

import re
from urllib.parse import urlparse, urlunparse

# Domains that are never a business website (social / review aggregators /
# share widgets). Substring match on the host, so regional variants are covered.
SOCIAL_DOMAINS = frozenset({
    "facebook.com", "instagram.com", "x.com", "twitter.com", "linkedin.com",
    "youtube.com", "tiktok.com", "yelp.com", "thumbtack.com",
    "angieslist.com", "homeadvisor.com", "porch.com", "houzz.com",
    "nextdoor.com", "yellowpages.com", "whitepages.com", "mapquest.com",
    "manta.com", "bbb.org",
})

# Pure tracking / analytics / CDN hosts that sometimes leak in as "websites".
TRACKING_DOMAINS = frozenset({
    "googletagmanager.com", "google-analytics.com", "doubleclick.net",
    "gstatic.com", "googleapis.com", "adobedtm.com", "livechatinc.com",
    "cloudflare.com", "jsdelivr.net", "unpkg.com", "schema.org",
})

BLOCKED_DOMAINS = SOCIAL_DOMAINS | TRACKING_DOMAINS


def _host_blocked(host: str) -> bool:
    return any(host == d or host.endswith("." + d) for d in BLOCKED_DOMAINS)


def is_social_url(url: str | None) -> bool:
    """True if the URL points at a social/review platform, not a business site."""
    if not url:
        return False
    try:
        host = urlparse(url if "://" in url else "https://" + url).hostname or ""
    except Exception:
        return False
    host = host.lower()
    return any(host == d or host.endswith("." + d) for d in SOCIAL_DOMAINS)


def extract_domain(url: str | None) -> str | None:
    """Return the lowercase host for a URL, or None if unparseable."""
    if not url or not isinstance(url, str):
        return None
    try:
        host = urlparse(url.strip() if url.strip().startswith(("http://", "https://"))
                        else "https://" + url.strip()).hostname or ""
    except Exception:
        return None
    return host.lower() or None


def normalize_website(url: str | None) -> str | None:
    """Clean a scraped business website. Returns None when unusable.

    - strips whitespace (incl. internal spaces, which are never valid)
    - prepends https:// when the scheme is missing
    - lowercases scheme + host
    - rejects: empty values, hosts without a dot, phone numbers stored
      as URLs, over-long values, social/tracking hosts
    """
    if not url or not isinstance(url, str):
        return None
    u = url.strip()
    if not u or len(u) > 500:
        return None
    if any(ch.isspace() for ch in u):
        return None
    if "://" not in u:
        # scheme-less: only accept if it looks like host[/path]
        if "." not in u.split("/")[0]:
            return None
        u = "https://" + u
    try:
        parts = urlparse(u)
    except Exception:
        return None
    if parts.scheme not in ("http", "https"):
        return None
    host = (parts.hostname or "").lower()
    if not host or "." not in host:
        return None
    # phone number stored as URL (digits/separators only, typical phone length)
    digits = re.sub(r"\D", "", host)
    if digits and len(digits) >= 7 and len(digits) <= 15 and not re.search(r"[a-z]", host):
        return None
    if _host_blocked(host):
        return None
    path = parts.path
    if path == "/":
        path = ""
    return urlunparse(("https" if parts.scheme == "https" else "http",
                       host, path, parts.params, parts.query, ""))
