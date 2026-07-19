"""Email extraction utilities — regex, normalization, noise filtering.

Supports standard emails, obfuscated patterns (common in BD business sites),
and mailto: link extraction. Used by both inline (browser-based) and offline
(HTTP-based) extraction pipelines.
"""

import re
import logging
from typing import Optional

log = logging.getLogger(__name__)

# ── Compiled patterns ────────────────────────────────────────────────────────

# Standard email: user@domain.tld
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9][a-zA-Z0-9._%+-]{0,63}@[a-zA-Z0-9][a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Obfuscated patterns used in Bangladesh business websites
# e.g. "info [at] company [dot] com", "info(at)company(dot)com"
OBFUSCATED_PATTERNS = [
    # Pattern 1: word [at] word [dot] tld
    re.compile(
        r"([a-zA-Z0-9._%+-]+)\s*\[?@?\]?\s*\[?at\]?\s*"
        r"([a-zA-Z0-9.-]+)\s*\[?\.?\]?\s*\[?dot\]?\s*"
        r"([a-zA-Z]{2,})",
        re.IGNORECASE,
    ),
    # Pattern 2: word(at)word(dot)tld
    re.compile(
        r"([a-zA-Z0-9._%+-]+)\s*\(at\)\s*"
        r"([a-zA-Z0-9.-]+)\s*\(dot\)\s*"
        r"([a-zA-Z]{2,})",
        re.IGNORECASE,
    ),
    # Pattern 3: word[at]word[dot]tld
    re.compile(
        r"([a-zA-Z0-9._%+-]+)\[at\]"
        r"([a-zA-Z0-9.-]+)\[dot\]"
        r"([a-zA-Z]{2,})",
        re.IGNORECASE,
    ),
    # Pattern 4: word AT word DOT tld
    re.compile(
        r"([a-zA-Z0-9._%+-]+)\s+AT\s+"
        r"([a-zA-Z0-9.-]+)\s+DOT\s+"
        r"([a-zA-Z]{2,})",
        re.IGNORECASE,
    ),
]

# mailto: link extraction
MAILTO_REGEX = re.compile(r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})')

# ── Noise filters ─────────────────────────────────────────────────────────────

NOISE_PREFIXES = {
    "noreply", "no-reply", "no_reply", "donotreply", "do-not-reply",
    "nobody", "example", "test", "localhost", "root", "admin",
    "webmaster", "hostmaster", "postmaster",
}

NOISE_DOMAINS = {
    "example.com", "example.net", "example.org", "test.com",
    "localhost.localdomain", "domain.com",
}

MIN_EMAIL_LENGTH = 8


# ── Public API ────────────────────────────────────────────────────────────────


def scan_text_for_emails(text: str) -> list[dict]:
    """Find all email addresses in text, both standard and obfuscated.

    Args:
        text: Raw page text or HTML source.

    Returns:
        List of dicts: {email, is_obfuscated, context_snippet}
    """
    results: list[dict] = []
    seen: set[str] = set()

    if not text:
        return results

    # 1. Standard emails
    for match in EMAIL_REGEX.finditer(text):
        raw = match.group(0).strip().lower()
        normalized = normalize_email(raw)
        if normalized and normalized not in seen:
            seen.add(normalized)
            start = max(0, match.start() - 40)
            end = min(len(text), match.end() + 40)
            results.append({
                "email": normalized,
                "is_obfuscated": False,
                "context_snippet": text[start:end].replace("\n", " ").strip(),
            })

    # 2. Obfuscated emails
    for pattern in OBFUSCATED_PATTERNS:
        for match in pattern.finditer(text):
            try:
                local = match.group(1).strip()
                domain = match.group(2).strip()
                tld = match.group(3).strip()
                reconstructed = f"{local}@{domain}.{tld}".lower()
                normalized = normalize_email(reconstructed)
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    start = max(0, match.start() - 40)
                    end = min(len(text), match.end() + 40)
                    results.append({
                        "email": normalized,
                        "is_obfuscated": True,
                        "context_snippet": text[start:end].replace("\n", " ").strip(),
                    })
            except IndexError:
                continue

    return results


def extract_mailto_links(html: str) -> list[str]:
    """Extract email addresses from mailto: links in HTML source.

    Args:
        html: Raw HTML source of the page.

    Returns:
        List of unique email addresses.
    """
    seen: set[str] = set()
    emails: list[str] = []
    for match in MAILTO_REGEX.finditer(html):
        email = match.group(1).strip().lower()
        normalized = normalize_email(email)
        if normalized and normalized not in seen:
            seen.add(normalized)
            emails.append(normalized)
    return emails


def normalize_email(raw: str) -> Optional[str]:
    """Normalize and validate an email address.

    Strips whitespace, lowercases, removes trailing dots.
    Returns None if the email is invalid or should be filtered.

    Args:
        raw: Raw email string.

    Returns:
        Normalized email string, or None if rejected.
    """
    if not raw or not isinstance(raw, str):
        return None

    email = raw.strip().lower()

    # Remove trailing/leading dots or special chars
    email = email.strip(".")

    # Basic validation
    if "@" not in email:
        return None

    if len(email) < MIN_EMAIL_LENGTH:
        return None

    parts = email.split("@")
    if len(parts) != 2:
        return None

    local, domain = parts

    if not local or not domain:
        return None

    if "." not in domain:
        return None

    # Reject file-extension TLDs (CSS/JS/SVG etc. are not valid email TLDs)
    tld = domain.rsplit(".", 1)[-1].lower()
    FILE_TLDS = {"css", "js", "json", "svg", "png", "jpg", "jpeg", "gif",
                 "woff", "woff2", "ttf", "eot", "ico", "webp", "mp4",
                 "mp3", "pdf", "zip", "tar", "gz"}
    if tld in FILE_TLDS:
        return None

    # Reject when domain's second-level part has no letters (e.g. @11.css, @123.com)
    domain_sl = domain.rsplit(".", 1)[0]  # e.g. "11" from "11.css"
    if not any(c.isalpha() for c in domain_sl):
        return None

    # Noise filter
    local_clean = local.replace(".", "").replace("-", "").replace("_", "")
    if local_clean in NOISE_PREFIXES:
        return None

    if domain in NOISE_DOMAINS:
        return None

    return email


def filter_noise(emails: list[dict]) -> list[dict]:
    """Remove low-quality email entries from a list.

    Filters out:
      - noreply / no-reply / donotreply addresses
      - example.com / test.com domains
      - Very short addresses

    Args:
        emails: List of email dicts from scan_text_for_emails().

    Returns:
        Filtered list.
    """
    return [e for e in emails if normalize_email(e.get("email", "")) is not None]


def deduplicate_emails(emails: list[dict]) -> list[dict]:
    """Deduplicate a list of email dicts by email address.

    Preserves first occurrence (earliest = most reliable extraction).

    Args:
        emails: List of email dicts.

    Returns:
        Deduplicated list.
    """
    seen: set[str] = set()
    result: list[dict] = []
    for e in emails:
        addr = e.get("email", "").lower()
        if addr and addr not in seen:
            seen.add(addr)
            result.append(e)
    return result
