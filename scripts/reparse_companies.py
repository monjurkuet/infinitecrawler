#!/usr/bin/env python3
"""Re-extract company_name from profile_title for all LinkedIn profiles."""

import argparse
import logging
import re
import sys
from pathlib import Path

import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from utils.pg import get_pg_config  # noqa: E402

log = logging.getLogger("reparse")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - reparse - %(levelname)s - %(message)s")


def extract_company(title):
    if not title:
        return None
    cleaned = re.sub(r'\s*(?:[·\\.\-\|]\s*)?(?:LinkedIn|Linkedin)\s*$', '', title, flags=re.IGNORECASE).strip()
    job_words = {'Manager','Director','Chef','Engineer','Analyst','CEO','CTO','CFO','President','Chairman','Vice','Founder','Officer','Lead','Head','Assistant','Senior','Executive','Supervisor','Team','Sales','Operations','Housekeeping','Front','Duty','Human','Resources','AssistantDirector','ExecutiveDirector','SalesDirector','SafetyDirector','BIMDirector','ProjectDirector','Fuel','Division'}
    words = set(cleaned.replace('-',' ').replace('.',' ').split())
    has_job = bool(words & job_words)
    if not has_job and ' at ' not in cleaned.lower() and not re.search(r'\bat[A-Z]', cleaned) and len(cleaned) > 5:
        return cleaned
    m = re.search(r'\bat\s*([A-Z][A-Za-z0-9&.()-]{2,80})', cleaned)
    if m:
        return m.group(1).strip()
    m = re.search(r'\s-\s+([A-Z][A-Za-z0-9&\s,.()-]{3,80})', cleaned)
    if m:
        return m.group(1).strip()
    m = re.search(r'(?:Director|Manager|Chef|Engineer|Analyst|Lead|Officer)at([A-Z][A-Za-z0-9]{2,80})', cleaned)
    if m:
        return m.group(1)
    m = re.search(r'(?:CEO|Founder|President|Chairman|CTO)\s+(?:and\s+|&\s+)?(?:Co-)?(?:Founder|CEO)?\s*(?:at\s+|-\s*)([A-Z][A-Za-z0-9&.\s-]{2,80}?)', cleaned)
    if m:
        return m.group(1).strip()
    known_brands = ['InterContinental','Radisson','Westin','Sonargaon','Sheraton','Renaissance','Marriott','Hilton','Holiday Inn','Crowne Plaza','Amari','Pan Pacific','Agrabad','Ocean Paradise','Royal Tulip','Le Meridien','Charuta','NAVANA','BRTC','WAB','Picco','Starbucks','Microsoft','Deloitte','British Council','Ascott','STRATEGIC','Save','Addiction']
    for brand in known_brands:
        if brand.lower() in cleaned.lower():
            idx = cleaned.lower().find(brand.lower())
            start = idx
            while start > 0 and cleaned[start-1] not in (' ','-','.','|'):
                if cleaned[max(0,start-2):start].lower() == 'at':
                    break
                start -= 1
            end = idx + len(brand)
            while end < len(cleaned) and cleaned[end] not in (' ','-','.','|'):
                chunk = cleaned[end:end+6].lower()
                if any(chunk.startswith(x) for x in ('marke','compa','corpo','indust')):
                    break
                end += 1
            val = cleaned[start:end].strip()
            if len(val) > 2:
                return val
            break
    return None

def update_companies(conn, dry_run):
    cur = conn.cursor()
    cur.execute("SELECT id, profile_title FROM scraper.luxury_contacts WHERE platform = 'linkedin' AND (company_name IS NULL OR company_name = '')")
    luxury = cur.fetchall()
    cur.execute("SELECT id, profile_title FROM scraper.discovered_profiles WHERE platform = 'linkedin' AND (company_name IS NULL OR company_name = '')")
    discovered = cur.fetchall()
    total = len(luxury) + len(discovered)
    log.info("Profiles: %d (luxury=%d, discovered=%d)", total, len(luxury), len(discovered))
    if dry_run:
        for tid, title in luxury[:30]:
            c = extract_company(title)
            if c:
                log.info("  [lux id=%d] '%s' -> '%s'", tid, (title or '')[:45], c)
        if len(luxury) > 30:
            log.info("  ... %d more luxury", len(luxury) - 30)
        for tid, title in discovered[:15]:
            c = extract_company(title)
            if c:
                log.info("  [disc id=%d] '%s' -> '%s'", tid, (title or '')[:45], c)
        return 0
    updated = 0
    for table, rows in [("scraper.luxury_contacts", luxury), ("scraper.discovered_profiles", discovered)]:
        for tid, title in rows:
            c = extract_company(title)
            if c:
                cur.execute(f"UPDATE {table} SET company_name = %s, last_checked = NOW() WHERE id = %s", (c, tid))
                updated += 1
    return updated


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    conn = psycopg.connect(**get_pg_config())
    conn.autocommit = True
    try:
        updated = update_companies(conn, args.dry_run)
        log.info("%s", "Dry run done" if args.dry_run else f"Updated {updated} profiles")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
