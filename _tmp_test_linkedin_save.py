#!/usr/bin/env python3
"""Test: parse LinkedIn results from DDGS and save a few test rows."""
import sys, json, re
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx, asyncio
import psycopg
from utils.pg import get_pg_config

DDGS = "https://search.datasolved.org/search/text"

async def test():
    async with httpx.AsyncClient(timeout=httpx.Timeout(20)) as client:
        q = 'site:linkedin.com/in/ "The Westin Dhaka" Dhaka'
        print(f"Query: {q}")
        resp = await client.get(DDGS, params={"query": q, "max_results": 8, "region": "bd-bn"})
        data = resp.json()
        results = data.get("results", [])
        print(f"Status: {resp.status_code}, Results: {len(results)}")
        
        parsed = []
        for r in results:
            href = r.get("href", "")
            if "linkedin.com/in/" not in href:
                continue
            
            # Normalize URL
            url = href.split("?")[0].rstrip("/")
            url = url.replace("https://bd.linkedin.com/", "https://www.linkedin.com/")
            
            title = r.get("title", "")
            body = r.get("body", "")
            
            # Extract name
            name = None
            for sep in [" - ", " | "]:
                parts = title.split(sep)
                if len(parts) > 1:
                    c = parts[0].strip()
                    if len(c) > 2 and not c.startswith("http"):
                        name = c
                        break
            if not name:
                name = title.split(" | ")[0].strip() if " | " in title else None
            
            # Extract title
            profile_title = None
            if " - " in title:
                middle = title.split(" - ", 1)[1]
                profile_title = re.sub(r"\s*\|.*$", "", middle).strip()
            
            # Extract company
            company = None
            if " at " in title:
                after = title.split(" at ", 1)[1]
                company = re.sub(r"\s*\|.*$", "", after).strip()
            if not company:
                m = re.search(r"([A-Z][A-Za-z0-9&. ]{2,60})\s*[·|•]\s*(Full|Self|Part)", body)
                if m:
                    company = m.group(1).strip()
            
            parsed.append({
                "full_name": name,
                "profile_url": url,
                "profile_title": profile_title,
                "company_name": company,
                "snippet": body[:300],
            })
            print(f"\n  Name: {name}")
            print(f"  URL: {url}")
            print(f"  Title: {profile_title}")
            print(f"  Company: {company}")
        
        # Save test rows
        print(f"\n--- Saving {len(parsed)} test rows to luxury_contacts (target 3=Westin) ---")
        pg = get_pg_config()
        conn = psycopg.connect(**pg)
        try:
            for i, p in enumerate(parsed):
                try:
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO scraper.luxury_contacts
                            (target_id, full_name, platform, profile_url, profile_title,
                             company_name, search_query, confidence, snippet, is_employee, is_guest)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (profile_url) DO UPDATE SET
                            confidence = GREATEST(scraper.luxury_contacts.confidence, EXCLUDED.confidence),
                            last_checked = NOW()
                    """, (3, p["full_name"], "linkedin", p["profile_url"], p["profile_title"],
                          p["company_name"], q, 0.5, p["snippet"], True, False))
                    conn.commit()
                    print(f"  [{i+1}] Saved: {p['full_name']}")
                except Exception as e:
                    conn.rollback()
                    print(f"  [{i+1}] ERROR: {e}")
            
            # Mark target as searched
            cur = conn.cursor()
            cur.execute("UPDATE scraper.luxury_targets SET linkedin_searched = TRUE, updated_at = NOW() WHERE id = 3")
            conn.commit()
            print("\n  Target 3 (Westin) marked as searched.")
            
        finally:
            conn.close()

asyncio.run(test())
