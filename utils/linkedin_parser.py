from __future__ import annotations

from typing import Optional  # noqa: F401  # kept for caller compatibility


def parse_linkedin(
    result: dict,
    parse_name,
    parse_title,
    parse_company,
    url_norm,
) -> dict | None:
    href = result.get("href", "")
    if not href.startswith("https://www.linkedin.com/in/") and not href.startswith("https://bd.linkedin.com/in/"):
        return None
    title = result.get("title", "")
    body = result.get("body", "")
    return {
        "full_name": parse_name(title),
        "profile_url": url_norm(href),
        "profile_title": parse_title(title, body),
        "company_name": parse_company(title, body),
        "snippet": body[:500],
        "confidence": 0.3,
        "platform": "linkedin",
    }