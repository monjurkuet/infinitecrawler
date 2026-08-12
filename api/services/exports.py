"""api/services/exports.py — CSV export endpoint helpers."""

from __future__ import annotations

import csv
import io

from api.services.leads_repo import _build_leads_where
from api.services.pg_pool import get_pool


async def export_leads_csv(filters: dict, limit: int = 0) -> str:
    """Return CSV content for matching leads. Optional limit for quick preview."""

    where_sql, where_params = _build_leads_where(filters)
    pool = await get_pool()

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            sql = (
                f"SELECT name, category, phone, website, social_links, address, rating, "
                f"review_count, latitude, longitude, place_id, source_url, sector_id "
                f"FROM scraper.gmaps_listings WHERE {where_sql} "
                f"ORDER BY review_count DESC NULLS LAST"
            )
            if limit > 0:
                sql += f" LIMIT {limit}"
            await cur.execute(sql, where_params)
            rows = await cur.fetchall()
            cols = ["Name", "Category", "Phone", "Website", "Social", "Address",
                    "Rating", "Reviews", "Lat", "Lng", "Place ID", "Source URL", "Sector"]

            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(cols)
            for r in rows:
                writer.writerow(r)
            return buf.getvalue()
