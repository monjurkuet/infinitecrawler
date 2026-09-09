"""utils/linkedin_jobs_parser.py — Parse LinkedIn guest API HTML responses.

Handles:
  - Search API page (seeMoreJobPostings/search) → extract job_ids
  - Job detail API page (jobPosting/{id}) → extract all fields
  - Company page (company/{slug}) → extend existing linkedin_enrich logic

No JavaScript execution; all data is in server-rendered HTML.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


# ── Search API response parsing ──────────────────────────────────────────────
JOB_URN_RE = re.compile(r'data-entity-urn="urn:li:jobPosting:(\d+)"')


def extract_job_ids_from_search(html: str) -> list[int]:
    """Return list of job_ids from a search results page.

    The guest API returns an HTML fragment with <li> cards, each having
    data-entity-urn="urn:li:jobPosting:{id}".
    """
    return [int(m) for m in JOB_URN_RE.findall(html)]


# ── Job detail page parsing ──────────────────────────────────────────────────

# Selectors for the jobPosting/{id} page
SELECTORS = {
    "title": ".top-card-layout__title, .job-details-jobs-unified-top-card__job-title",
    "company_link": ".topcard__org-name-link, .job-details-jobs-unified-top-card__company-name a, a[href*='/company/'][trk*='public_jobs_topcard']",
    "company_name": ".topcard__org-name-link, .job-details-jobs-unified-top-card__company-name",
    "location": ".topcard__flavor--bullet, .job-details-jobs-unified-top-card__primary-description-container .topcard__flavor",
    "listed_time": ".posted-time-ago__text, .job-details-jobs-unified-top-card__posted-date",
    "description_html": ".show-more-less-html__markup--clamp-after-5, .show-more-less-html__markup, .jobs-description__content",
    "description_text": ".show-more-less-html__markup, .jobs-description__content",
    "criteria_list": ".description__job-criteria-list, .job-criteria__list",
    "criteria_item": ".description__job-criteria-item, .job-criteria__item",
    "criteria_label": ".description__job-criteria-subheader, .job-criteria__label",
    "criteria_value": ".description__job-criteria-text, .job-criteria__value",
    "applicants": ".num-applicants__caption, .job-details-how-you-match__applicants",
    "apply_button": "[data-tracking-control-name='public_jobs_apply-link-offsite'], .jobs-apply-button",
    "promoted": ".job-search-card__promoted-badge, .promoted-badge",
}

# Criteria label mapping
CRITERIA_MAP = {
    "seniority level": "seniority_level",
    "employment type": "employment_type",
    "job function": "job_function",
    "industries": "industries",
}


def parse_relative_time(text: str) -> datetime | None:
    """Parse LinkedIn's relative time strings to UTC datetime.

    Examples: "2 hours ago", "3 days ago", "1 week ago", "1 month ago",
    "just now", "30+ days ago".
    """
    if not text:
        return None
    text = text.lower().strip()
    if "just now" in text or "moments ago" in text:
        return datetime.now(timezone.utc)

    # Handle "X time ago" pattern
    patterns = [
        (r"(\d+)\s*second", lambda x: x),
        (r"(\d+)\s*minute", lambda x: x * 60),
        (r"(\d+)\s*hour", lambda x: x * 3600),
        (r"(\d+)\s*day", lambda x: x * 86400),
        (r"(\d+)\s*week", lambda x: x * 604800),
        (r"(\d+)\s*month", lambda x: x * 2592000),  # approximate
        (r"(\d+)\s*year", lambda x: x * 31536000),
    ]

    for pattern, mult in patterns:
        m = re.search(pattern, text)
        if m:
            seconds = mult(int(m.group(1)))
            return datetime.fromtimestamp(
                datetime.now(timezone.utc).timestamp() - seconds, tz=timezone.utc
            )

    # "30+ days ago" or similar
    m = re.search(r"(\d+)\+?\s*days?", text)
    if m:
        days = int(m.group(1))
        return datetime.fromtimestamp(
            datetime.now(timezone.utc).timestamp() - days * 86400, tz=timezone.utc
        )

    log.debug("Could not parse relative time: %s", text)
    return None


def parse_applicants(text: str) -> int | None:
    """Parse '123 applicants' or '1.2k applicants' to integer."""
    if not text:
        return None
    text = text.lower().replace(",", "").strip()
    m = re.search(r"([\d.]+)\s*([km]?)", text)
    if not m:
        return None
    val = float(m.group(1))
    suffix = m.group(2)
    if suffix == "k":
        val *= 1000
    elif suffix == "m":
        val *= 1_000_000
    return int(val)


@dataclass
class ParsedJob:
    job_id: int
    source_url: str
    title: str = ""
    company_name: str = ""
    company_linkedin_slug: str = ""
    company_linkedin_url: str = ""
    location: str = ""
    location_city: str = ""
    listed_at: datetime | None = None
    seniority_level: str = ""
    employment_type: str = ""
    job_function: str = ""
    industries: str = ""
    description_html: str = ""
    description_text: str = ""
    applicants_count: int | None = None
    promoted: bool = False
    easy_apply: bool = False
    apply_url: str = ""
    source_sector: str = ""
    source_keyword: str = ""
    raw_html: str = ""


def parse_job_detail(html: str, job_id: int, keyword: str, sector: str | None,
                     location: str) -> ParsedJob:
    """Parse a full job detail page into a structured object."""
    soup = BeautifulSoup(html, "lxml")

    # Title
    title_elem = soup.select_one(SELECTORS["title"])
    title = title_elem.get_text(strip=True) if title_elem else ""

    # Company link + slug
    company_link_elem = soup.select_one(SELECTORS["company_link"])
    company_name = ""
    company_linkedin_slug = ""
    company_linkedin_url = ""
    if company_link_elem:
        # The selector might return the <a> directly (.topcard__org-name-link)
        # or an element containing an <a>
        if company_link_elem.name == "a":
            company_name = company_link_elem.get_text(strip=True)
            href = company_link_elem.get("href", "")
        else:
            company_name = company_link_elem.get_text(strip=True)
            link_child = company_link_elem.find("a")
            href = link_child.get("href", "") if link_child else ""
        # href looks like /company/hijazhajjnumrah/ or /company/hijazhajjnumrah/?trk=...
        # or https://bd.linkedin.com/company/hijazhajjnumrah?trk=...
        m = re.search(r"/company/([^/?]+)", href)
        if m:
            company_linkedin_slug = m.group(1)
            company_linkedin_url = f"https://www.linkedin.com/company/{company_linkedin_slug}/"

    # Fallback: company name from non-link element
    if not company_name:
        company_elem = soup.select_one(SELECTORS["company_name"])
        if company_elem:
            company_name = company_elem.get_text(strip=True)

    # Location
    loc_elem = soup.select_one(SELECTORS["location"])
    raw_location = loc_elem.get_text(strip=True) if loc_elem else ""
    # Normalize city
    location_city = ""
    if "dhaka" in raw_location.lower() or "dacca" in raw_location.lower():
        location_city = "Dhaka"
    elif "chattogram" in raw_location.lower() or "chittagong" in raw_location.lower():
        location_city = "Chattogram"
    elif "bangladesh" in raw_location.lower():
        location_city = "Bangladesh"

    # Listed time
    time_elem = soup.select_one(SELECTORS["listed_time"])
    listed_at = None
    if time_elem:
        listed_at = parse_relative_time(time_elem.get_text(strip=True))

    # Description
    desc_html_elem = soup.select_one(SELECTORS["description_html"])
    description_html = str(desc_html_elem) if desc_html_elem else ""
    description_text = desc_html_elem.get_text("\n", strip=True) if desc_html_elem else ""

    # Criteria (seniority, employment type, job function, industries)
    criteria = {"seniority_level": "", "employment_type": "", "job_function": "", "industries": ""}
    criteria_list = soup.select_one(SELECTORS["criteria_list"])
    if criteria_list:
        for item in criteria_list.select(SELECTORS["criteria_item"]):
            label_elem = item.select_one(SELECTORS["criteria_label"])
            value_elem = item.select_one(SELECTORS["criteria_value"])
            if label_elem and value_elem:
                label = label_elem.get_text(strip=True).lower()
                value = value_elem.get_text(strip=True)
                if label in CRITERIA_MAP:
                    criteria[CRITERIA_MAP[label]] = value

    # Applicants
    app_elem = soup.select_one(SELECTORS["applicants"])
    applicants_count = parse_applicants(app_elem.get_text(strip=True)) if app_elem else None

    # Promoted
    promoted_elem = soup.select_one(SELECTORS["promoted"])
    promoted = promoted_elem is not None

    # Easy apply / apply URL (often requires JS, may not be in static HTML)
    apply_elem = soup.select_one(SELECTORS["apply_button"])
    easy_apply = False
    apply_url = ""
    if apply_elem:
        easy_apply = True
        href = apply_elem.get("href")
        if href and isinstance(href, str) and href.startswith("http"):
            apply_url = href

    return ParsedJob(
        job_id=job_id,
        source_url=f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}",
        title=title,
        company_name=company_name,
        company_linkedin_slug=company_linkedin_slug,
        company_linkedin_url=company_linkedin_url,
        location=raw_location,
        location_city=location_city,
        listed_at=listed_at,
        seniority_level=criteria["seniority_level"],
        employment_type=criteria["employment_type"],
        job_function=criteria["job_function"],
        industries=criteria["industries"],
        description_html=description_html,
        description_text=description_text,
        applicants_count=applicants_count,
        promoted=promoted,
        easy_apply=easy_apply,
        apply_url=apply_url,
        source_sector=sector or "",
        source_keyword=keyword,
        raw_html=html[:50000],  # truncate for storage
    )


# ── Company page parsing (extends linkedin_enrich logic) ─────────────────────

def parse_company_page(html: str, slug: str) -> dict[str, Any]:
    """Parse a company page, returning a dict compatible with upsert_linkedin_company.

    Tries JSON-LD first (Organization schema), falls back to HTML extraction.
    """
    soup = BeautifulSoup(html, "lxml")

    # Try JSON-LD first
    ld_result = _extract_org_from_json_ld(soup)
    if ld_result:
        ld_result["slug"] = slug
        ld_result["linkedin_url"] = f"https://www.linkedin.com/company/{slug}/"
        return ld_result

    # Fallback: HTML extraction (less complete)
    return _extract_company_from_html(soup, slug)


def _extract_org_from_json_ld(soup: BeautifulSoup) -> dict[str, Any] | None:
    """Extract Organization from ld+json script tags."""
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            script_text = script.string
            if not script_text:
                continue
            data = json.loads(script_text)
        except (json.JSONDecodeError, TypeError):
            continue

        # @graph array or single object
        graphs = data.get("@graph", [data]) if isinstance(data, dict) else []
        for g in graphs:
            if isinstance(g, dict) and g.get("@type") == "Organization":
                org = g
                addr = org.get("address", {})
                headquarters_parts = []
                if isinstance(addr, dict):
                    for k in ("streetAddress", "addressLocality", "addressRegion", "postalCode", "addressCountry"):
                        if addr.get(k):
                            headquarters_parts.append(addr[k])
                headquarters = ", ".join(headquarters_parts) if headquarters_parts else ""

                same_as = org.get("sameAs")
                if isinstance(same_as, list):
                    website = same_as[0] if same_as else ""
                else:
                    website = same_as or ""

                logo = org.get("logo", {})
                logo_url = logo.get("contentUrl") if isinstance(logo, dict) else (logo or "")

                num_emp = org.get("numberOfEmployees", {})
                employee_count = num_emp.get("value") if isinstance(num_emp, dict) else None

                return {
                    "company_name": org.get("name", ""),
                    "slug": "",
                    "linkedin_url": org.get("url", ""),
                    "industry": org.get("industry", ""),
                    "company_size": "",  # not in schema
                    "employee_count": employee_count,
                    "followers": None,
                    "headquarters": headquarters,
                    "website": website,
                    "founded": None,
                    "specialties": [],
                    "description": org.get("description", ""),
                    "logo_url": logo_url,
                    "notes": "json_ld",
                }
    return None


def _extract_company_from_html(soup: BeautifulSoup, slug: str) -> dict[str, Any]:
    """Fallback HTML extraction when JSON-LD is absent."""
    # Title from top card
    name = ""
    title_elem = soup.select_one(".top-card-layout__title, .org-top-card__primary-content h1")
    if title_elem:
        name = title_elem.get_text(strip=True)

    # Industry / location line
    industry = ""
    location_line = ""
    for elem in soup.select(".top-card-layout__first-subline, .org-top-card__primary-content .top-card-layout__entity-info"):
        text = elem.get_text(strip=True)
        if text and not industry:
            industry = text
        elif text and not location_line:
            location_line = text

    # Employee count from insight text
    employee_count = None
    for elem in soup.find_all(string=re.compile(r"employees|staff|associates", re.I)):
        parent = elem.parent
        if parent:
            m = re.search(r"([\d,]+)\s*(?:employees?|staff|associates)", parent.get_text(), re.I)
            if m:
                employee_count = int(m.group(1).replace(",", ""))
                break

    # Description from about section
    description = ""
    about_elem = soup.select_one(".org-about-us, .about-us__description, [data-test-id='about-us']")
    if about_elem:
        description = about_elem.get_text("\n", strip=True)

    # Website from 'sameAs' or link
    website = ""
    for link in soup.select("a[href^='http']"):
        href = link.get("href")
        if href and isinstance(href, str) and "linkedin.com" not in href and any(x in href for x in [".com", ".org", ".net", ".bd"]):
            website = href
            break

    return {
        "company_name": name,
        "slug": slug,
        "linkedin_url": f"https://www.linkedin.com/company/{slug}/",
        "industry": industry,
        "company_size": "",
        "employee_count": employee_count,
        "followers": None,
        "headquarters": location_line,
        "website": website,
        "founded": None,
        "specialties": [],
        "description": description,
        "logo_url": "",
        "notes": "html_fallback",
    }


if __name__ == "__main__":
    # Quick test with cached sample
    import sys
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            html = f.read()
        job = parse_job_detail(html, 4442968734, "test", "test", "Dhaka, Bangladesh")
        print(f"Title: {job.title}")
        print(f"Company: {job.company_name} (slug: {job.company_linkedin_slug})")
        print(f"Location: {job.location} -> city: {job.location_city}")
        print(f"Listed: {job.listed_at}")
        print(f"Criteria: {job.seniority_level} | {job.employment_type} | {job.job_function} | {job.industries}")
        print(f"Applicants: {job.applicants_count}")
        print(f"Promoted: {job.promoted}")