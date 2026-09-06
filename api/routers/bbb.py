"""BBB scraper admin routes - jobs, leads, stats."""
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone
import psycopg

router = APIRouter(prefix="/api/bbb", tags=["bbb"])


def get_db():
    conn = psycopg.connect(
        host="/var/run/postgresql", dbname="infinitecrawler", user="postgres"
    )
    return conn


@router.get("/jobs")
def list_bbb_jobs():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, job_type, source, keyword, location, status,
                       rows_written, created_at
                FROM scraper.scrape_jobs
                ORDER BY created_at DESC LIMIT 50
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


@router.get("/leads")
def list_bbb_leads(
    state: Optional[str] = None,
    keyword: Optional[str] = None,
    limit: int = 50,
):
    with get_db() as conn:
        with conn.cursor() as cur:
            q = """
                SELECT id, business_id, business_name, address, city, state,
                       zip, phone, rating, accredited, source_query, created_at
                FROM scraper.bbb_listings WHERE 1=1
            """
            params = []
            if state:
                q += " AND state=%s"
                params.append(state)
            if keyword:
                q += " AND (source_query ILIKE %s OR business_name ILIKE %s)"
                params.extend(["%"+keyword+"%", "%"+keyword+"%"])
            q += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)
            cur.execute(q, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


@router.get("/stats")
def bbb_stats():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM scraper.bbb_listings")
            total = cur.fetchone()[0]
            cur.execute("SELECT state, count(*) FROM scraper.bbb_listings GROUP BY state ORDER BY 2 DESC LIMIT 5")
            by_state = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT source_query, count(*) FROM scraper.bbb_listings GROUP BY source_query ORDER BY 2 DESC LIMIT 10")
            by_source = {r[0]: r[1] for r in cur.fetchall()}
            return {"total": total, "by_state": by_state, "by_source": by_source}
