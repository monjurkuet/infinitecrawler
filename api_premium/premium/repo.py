"""Premium data layer — all queries parameterized, psycopg list-friendly."""
from typing import Any

from api_premium.premium import schemas


def build_where(
    city: str | None,
    category: str | None,
    min_rating: float | None,
    has_email: bool | None,
    q: str | None,
    country: str | None = None,
) -> tuple[str, list[Any]]:
    where = ["l.source_type='gmaps_listing'"]
    params: list[Any] = []
    if country:
        patterns = COUNTRY_PATTERNS.get(country)
        if patterns:
            if isinstance(patterns, str):
                patterns = [patterns]
            ors = " OR ".join("l.address ILIKE %s" for _ in patterns)
            where.append(f"({ors})")
            params.extend(patterns)
    if city:
        alias_group = BD_CITY_ALIASES.get(city)
        if alias_group:
            ors = " OR ".join("l.address ILIKE %s" for _ in alias_group)
            where.append(f"({ors})")
            for v in alias_group:
                params.append(f"%{v}%")
        else:
            where.append("l.address ILIKE %s")
            params.append(f"%{city}%")
    if category:
        where.append("l.category ILIKE %s")
        params.append(f"%{category}%")
    if min_rating is not None:
        where.append("l.rating >= %s")
        params.append(min_rating)
    if has_email is True:
        where.append(
            "EXISTS (SELECT 1 FROM scraper.emails e WHERE e.listing_id = l.id)"
        )
    if q:
        where.append("(l.name ILIKE %s OR l.address ILIKE %s OR l.category ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])
    return " AND ".join(where), params


LIST_SELECT = """
SELECT l.id, l.place_id, l.source_url, l.source_type, l.name, l.category, l.rating,
       l.review_count, l.address, l.phone, l.website, l.booking_url, l.plus_code,
       l.is_claimed, l.latitude, l.longitude, l.sector_id,
       l.classification_confidence, l.classification_method,
       l.created_at, l.updated_at, l.email_scanned_at,
       COALESCE(
           (SELECT array_agg(DISTINCT e.email ORDER BY e.email)
              FROM scraper.emails e WHERE e.listing_id = l.id), '{}'
       ) AS emails,
       (SELECT lp.profile_url FROM scraper.linkedin_profiles lp
         WHERE lp.listing_id = l.id ORDER BY lp.checked_at DESC LIMIT 1) AS linkedin_url,
       (SELECT lp.profile_title FROM scraper.linkedin_profiles lp
         WHERE lp.listing_id = l.id ORDER BY lp.checked_at DESC LIMIT 1) AS linkedin_title
FROM scraper.gmaps_listings l
"""

# City aliases — one canonical label maps to all spellings we've seen in the
# data (English + Bengali + transliterations). Keys are the UI dropdown values;
# values are SQL patterns ORed in the WHERE clause.
BD_CITY_ALIASES = {
    "Dhaka": ["Dhaka", "ঢাকা"],
    "Chattogram": ["Chattogram", "Chittagong", "চট্টগ্রাম"],
    "Sylhet": ["Sylhet", "সিলেট"],
    "Rajshahi": ["Rajshahi", "রাজশাহী"],
    "Khulna": ["Khulna", "খুলনা"],
    "Barisal": ["Barisal", "Barishal", "বরিশাল"],
    "Rangpur": ["Rangpur", "রংপুর"],
    "Mymensingh": ["Mymensingh", "ময়মনসিংহ"],
    "Gazipur": ["Gazipur", "গাজীপুর"],
    "Narayanganj": ["Narayanganj", "নারায়ণগঞ্জ"],
}

# Countries we actually have meaningful volumes for. Anything else falls into
# the India/UK/USA style ILIKE-of-address filter (good enough at current scale).
COUNTRY_PATTERNS = {
    "Bangladesh": "%Bangladesh%",
    "India": "%India%",
    "Canada": "%Canada%",
    "United Kingdom": ["%United Kingdom%", "%UK%"],
    "United States": ["%United States%", "%USA%"],
}


DETAIL_SELECT = """
SELECT l.*, l.payload,
       COALESCE(
           (SELECT array_agg(row_to_json(e)) FROM (
               SELECT email, is_obfuscated, extraction_method, discovered_at
               FROM scraper.emails WHERE listing_id = l.id ORDER BY discovered_at DESC
           ) e), '{}'
       ) AS emails_full,
       COALESCE(
           (SELECT array_agg(row_to_json(p)) FROM (
               SELECT profile_url, full_name, profile_title, company_name,
                      profile_location, headline, checked_at
               FROM scraper.linkedin_profiles WHERE listing_id = l.id
               ORDER BY checked_at DESC
           ) p), '{}'
       ) AS linkedin_profiles
FROM scraper.gmaps_listings l
WHERE l.id = %s
"""


CSV_COLUMNS = [
    "id", "name", "category", "rating", "review_count", "address",
    "phone", "website", "emails", "linkedin_url", "latitude", "longitude",
    "plus_code", "is_claimed", "sector_id", "created_at", "updated_at",
]
