#!/usr/bin/env python3
"""pipeline_manager.py — Orchestrates BPT → GMaps → Leads pipeline.

Flow:
  1. Reads BPT sector configs (sectors.yaml) to get verticals + subsegments
  2. Generates Google Maps search queries for each vertical (targeting cities outside Dhaka)
  3. Writes queries to input/search_queries_bd_businesses.txt
  4. Runs search crawler (discovers businesses)
  5. Exports uncrawled URLs and runs listing crawler (extracts phone/FB/website)
  6. Generates leads CSV grouped by BPT sector

Usage:
    python pipeline_manager.py                  # Full pipeline
    python pipeline_manager.py --generate-only  # Step 1-3 only
    python pipeline_manager.py --crawl-only     # Steps 4-6 only
    python pipeline_manager.py --dry-run        # Show what would be done
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("pipeline_manager")

REPO_ROOT = Path(__file__).resolve().parent.parent.parent  # ~/codebase/vhd
INFINITECRAWLER_DIR = REPO_ROOT / "infinitecrawler"
BPT_DIR = REPO_ROOT / "business-plan-template"
BPT_SYSTEM_DIR = BPT_DIR / "_system"
BPT_CONFIG = BPT_SYSTEM_DIR / "config"

# Target cities outside Dhaka (major divisional + commercial cities)
TARGET_CITIES = [
    ("Chattogram", "চট্টগ্রাম"),
    ("Sylhet", "সিলেট"),
    ("Khulna", "খুলনা"),
    ("Rajshahi", "রাজশাহী"),
    ("Barishal", "বরিশাল"),
    ("Rangpur", "রংপুর"),
    ("Mymensingh", "ময়মনসিংহ"),
    ("Cumilla", "কুমিল্লা"),
    ("Bogura", "বগুড়া"),
    ("Jashore", "যশোর"),
    ("Cox's Bazar", "কক্সবাজার"),
    ("Narayanganj", "নারায়ণগঞ্জ"),
    ("Gazipur", "গাজীপুর"),
    ("Feni", "ফেনী"),
    ("Narsingdi", "নরসিংদী"),
]

OUTPUT_DIR = INFINITECRAWLER_DIR / "output" / "leads_by_sector"


def load_yaml_simple(path: Path) -> dict:
    """Load YAML (no dependency needed)."""
    import yaml
    return yaml.safe_load(path.read_text())


def get_sectors() -> dict:
    """Load BPT sector configs."""
    path = BPT_CONFIG / "sectors.yaml"
    if not path.exists():
        log.error(f"sectors.yaml not found at {path}")
        return {}
    data = load_yaml_simple(path)
    return data.get("sectors", {})


def generate_search_queries(sectors: dict) -> list[str]:
    """Generate Google Maps search queries from BPT sector configs.
    
    For each sector, creates queries like:
    - "clothing boutique in Sylhet"
    - "clothing shop in Khulna"
    - "ফ্যাশন হাউস চট্টগ্রাম"  (bangla)
    
    BIM sector: BD-local keywords get city-level queries, global/BIM outsourcing
    keywords get Bangladesh-level + international queries.
    """
    queries = set()
    
    # International markets for BIM outsourcing
    INTERNATIONAL_MARKETS = ["USA", "UK", "Australia", "Canada", "UAE", "Saudi Arabia"]
    
    # BIM keywords that should NOT get city-level queries (global market targeting)
    BIM_GLOBAL_KEYWORDS = {
        "bim outsourcing", "revit modeling service", "mep coordination",
        "bim consultant", "architectural bim service", "scan to bim",
        "construction documentation", "bim production partner",
        "bim consulting", "scan-to-bim",
    }
    
    for sector_id, sector_config in sectors.items():
        if sector_config.get("status") != "active":
            continue
        
        # Determine if this is a truly global sector (pure international outreach)
        is_global = any(region in sector_id for region in ["global"]) and "bim" not in sector_id
        is_bim = "bim" in sector_id
        
        # Get all keywords
        keywords = []
        kw_dict = sector_config.get("keywords", {})
        keywords.extend(kw_dict.get("en", []))
        keywords.extend(kw_dict.get("bn", []))
        
        # Add subsegment-based queries
        subsegments = sector_config.get("subsegments", [])
        for sub in subsegments:
            keywords.append(sub)
        
        # Generate queries per city
        if not is_global:
            for keyword in keywords:
                # For BIM sector, skip city-level for global outsourcing keywords
                if is_bim and keyword.lower().strip() in BIM_GLOBAL_KEYWORDS:
                    continue
                for city_en, city_bn in TARGET_CITIES:
                    queries.add(f"{keyword} in {city_en}")
                    queries.add(f"{keyword} {city_bn}")
        
        # Bangladesh-level queries (for all keywords including BIM global)
        for keyword in keywords:
            queries.add(f"{keyword} Bangladesh")
            if "Bangladesh" not in keyword and "BD" not in keyword:
                queries.add(f"{keyword} outside Dhaka")
        
        # BIM global outsourcing: international market queries
        if is_bim:
            for keyword in keywords:
                kw_lower = keyword.lower().strip()
                if kw_lower in BIM_GLOBAL_KEYWORDS:
                    for market in INTERNATIONAL_MARKETS:
                        queries.add(f"{keyword} {market}")
                        queries.add(f"{keyword} {market} outsourcing")
    
    # Deduplicate and clean
    cleaned = []
    seen = set()
    for q in queries:
        # Normalize
        norm = q.strip().lower()
        norm = re.sub(r'\s+', ' ', norm)
        if norm and norm not in seen and len(norm) > 5:
            seen.add(norm)
            cleaned.append(q.strip())
    
    log.info(f"Generated {len(cleaned)} search queries from {len(sectors)} sectors")
    return sorted(cleaned)


def write_queries_file(queries: list[str]) -> Path:
    """Write queries to the search input file."""
    query_file = INFINITECRAWLER_DIR / "input" / "search_queries_bd_businesses.txt"
    query_file.parent.mkdir(parents=True, exist_ok=True)
    query_file.write_text("\n".join(queries) + "\n")
    log.info(f"Wrote {len(queries)} queries to {query_file}")
    return query_file


def run_search_crawler(dry_run: bool = False) -> bool:
    """Launch the GMaps search crawler in background (non-blocking).
    
    Returns True immediately after launching. The crawler runs as a background
    process and will be picked up on the next pipeline run.
    """
    if dry_run:
        log.info("[DRY-RUN] Would run: uv run python main.py --config config/gmaps_bd_business_search.yaml --headless")
        return True
    
    log.info("Starting GMaps search crawler in background...")
    # Clear the search queue first
    subprocess.run(
        ["redis-cli", "DEL", "gmaps_bd_business:pending", "gmaps_bd_business:processing", "gmaps_bd_business:failed"],
        capture_output=True
    )
    
    process = subprocess.Popen(
        ["uv", "run", "python", "main.py", "--config", "config/gmaps_bd_business_search.yaml", "--headless"],
        cwd=str(INFINITECRAWLER_DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    log.info(f"Search crawler launched in background (PID {process.pid})")
    return True


def export_uncrawled_urls(dry_run: bool = False) -> int:
    """Export uncrawled URLs for the listing crawler."""
    if dry_run:
        log.info("[DRY-RUN] Would export uncrawled URLs from PG")
        return 0
    
    result = subprocess.run(
        ["bash", "-c", 
         'PGPASSWORD=changeme psql -h 100.92.181.21 -U postgres -d infinitecrawler -c "COPY (SELECT DISTINCT sr.payload->>\'url\' FROM scraper.gmaps_search_results sr LEFT JOIN scraper.gmaps_listings gl ON gl.source_url = sr.payload->>\'url\' WHERE sr.payload->>\'url\' IS NOT NULL AND gl.source_url IS NULL ORDER BY 1) TO STDOUT;" > input/uncrawled_urls.txt'],
        cwd=str(INFINITECRAWLER_DIR),
        capture_output=True, text=True, timeout=60
    )
    
    outfile = INFINITECRAWLER_DIR / "input" / "uncrawled_urls.txt"
    if outfile.exists():
        count = len(outfile.read_text().strip().splitlines())
        log.info(f"Exported {count} uncrawled URLs")
        return count
    return 0


def run_listing_crawler(instances: int = 3, dry_run: bool = False) -> bool:
    """Launch the GMaps listing crawler in background (non-blocking).
    
    Returns True immediately after launching. The crawler runs as a background
    process managed by the health watchdog.
    """
    if dry_run:
        log.info(f"[DRY-RUN] Would run listing crawler with {instances} instances")
        return True
    
    log.info(f"Starting listing crawler ({instances} instances) in background...")
    # Clear listing queue (but NOT completed!)
    subprocess.run(
        ["redis-cli", "DEL", "gmaps:pending", "gmaps:processing", "gmaps:failed"],
        capture_output=True
    )
    
    process = subprocess.Popen(
        ["uv", "run", "python", "scripts/run_listing_crawlers.py",
         "--instances", str(instances),
         "--config", "config/gmaps_listings_working.yaml",
         "--headless"],
        cwd=str(INFINITECRAWLER_DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    log.info(f"Listing crawler launched in background (PID {process.pid})")
    return True


def generate_sector_leads(dry_run: bool = False) -> dict:
    """Generate leads CSV grouped by BPT sector using LLM classifier.
    
    Reads the gmaps_listings table, classifies each business via LLM,
    and writes per-sector lead CSVs.

    Deprecated: this is a one-shot prototype that classifies only max_leads=50
    per run and is not wired into the 24/7 daemon pipeline. Prefer db_classify.py
    (offline cron) or in-stream fallback in listing_daemon.py.
    """
    import warnings
    warnings.warn(
        "generate_sector_leads() is a deprecated prototype not used by the "
        "24/7 pipeline. Use scripts/db_classify.py instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    try:
        import psycopg
    except ImportError:
        log.error("psycopg not installed")
        return {}
    
    sectors = get_sectors()
    
    if dry_run:
        conn = None
    else:
        conn = psycopg.connect(
            host="100.92.181.21",
            port=5432,
            user="postgres",
            password="changeme",
            dbname="infinitecrawler",
        )
        conn.autocommit = True
    
    try:
        if conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, name, category, phone, website, address, rating,
                           review_count, latitude, longitude, place_id, source_url
                    FROM scraper.gmaps_listings
                    WHERE phone IS NOT NULL
                      AND website IS NOT NULL
                    ORDER BY review_count DESC NULLS LAST
                """)
                rows = cur.fetchall()
                cols = ["id", "name", "category", "phone", "website", "address",
                        "rating", "review_count", "lat", "lng", "place_id", "source_url"]
                leads = [dict(zip(cols, r)) for r in rows]
        else:
            leads = []
        
        # Use LLM classifier
        sys.path.insert(0, str(INFINITECRAWLER_DIR))
        from scripts.llm_classifier import classify_all, load_training_examples
        
        existing_examples = load_training_examples()
        classifications = classify_all(leads, sectors, existing_examples, dry_run=dry_run, max_leads=50)
        
        # Build sector → leads mapping
        sector_leads: dict[str, list] = {
            sid: [] for sid, sc in sectors.items() if sc.get("status") == "active"
        }
        sector_leads.setdefault("high-roi-niches", [])
        
        class_by_index = {c["index"]: c for c in classifications}
        unmapped = []
        
        for i, lead in enumerate(leads):
            cl = class_by_index.get(i, {})
            sector = cl.get("sector", "high-roi-niches")
            if sector in sector_leads:
                sector_leads[sector].append(lead)
            else:
                sector_leads["high-roi-niches"].append(lead)
                unmapped.append(lead)
        
        # Write per-sector CSVs
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        out_dir = OUTPUT_DIR / today
        if not dry_run:
            out_dir.mkdir(parents=True, exist_ok=True)
        
        summary = {}
        for sid, sl in sector_leads.items():
            if not sl:
                continue
            display = sectors.get(sid, {}).get("display_name", sid)
            
            if dry_run:
                log.info(f"  [{sid}] {display}: {len(sl)} leads")
                summary[sid] = {"display": display, "count": len(sl)}
                continue
            
            # Write CSV
            csv_path = out_dir / f"{sid}.csv"
            import csv as csv_mod
            with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv_mod.writer(f)
                writer.writerow(["Name", "Category", "Phone", "Website", "Has Facebook",
                                "Address", "Rating", "Reviews", "Lat", "Lng", "Place ID", "Source URL"])
                for lead in sl:
                    has_fb = "facebook" in (lead.get("website") or "").lower()
                    writer.writerow([
                        lead.get("name"), lead.get("category"), lead.get("phone"),
                        lead.get("website"), "Yes" if has_fb else "No",
                        lead.get("address"), lead.get("rating"), lead.get("review_count"),
                        lead.get("lat"), lead.get("lng"), lead.get("place_id"), lead.get("source_url"),
                    ])
            
            summary[sid] = {"display": display, "count": len(sl), "file": str(csv_path)}
        
        # Write summary
        if not dry_run:
            summary_path = out_dir / "summary.json"
            summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
        
        log.info(f"Lead generation complete. {len(leads)} leads across {len(summary)} sectors.")
        if unmapped:
            log.info(f"  {len(unmapped)} leads unclassified — placed in 'high-roi-niches'")
        
        return summary
    
    finally:
        if conn:
            conn.close()


def generate_query_report(queries: list[str], sectors: dict) -> str:
    """Generate a human-readable report of what queries were generated."""
    lines = ["📊 **Search Queries Generated by Sector**", ""]
    
    # Group queries by sector keyword match
    for sid, sc in sorted(sectors.items()):
        if sc.get("status") != "active":
            continue
        display = sc.get("display_name", sid)
        
        # Count queries matching this sector
        sector_queries = []
        kw_dict = sc.get("keywords", {})
        all_words = set()
        for kw in kw_dict.get("en", []) + kw_dict.get("bn", []):
            all_words.update(kw.lower().split())
        for sub in sc.get("subsegments", []):
            all_words.update(sub.lower().split())
        
        for q in queries:
            ql = q.lower()
            if any(w in ql for w in all_words):
                sector_queries.append(q)
        
        lines.append(f"**{display}**: {len(sector_queries)} queries")
        for q in sector_queries[:3]:
            lines.append(f"  · {q}")
        if len(sector_queries) > 3:
            lines.append(f"  · … and {len(sector_queries)-3} more")
        lines.append("")
    
    lines.append(f"**Total**: {len(queries)} queries across {sum(1 for s in sectors.values() if s.get('status')=='active')} active sectors")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="BPT → GMaps → Leads pipeline")
    parser.add_argument("--generate-only", action="store_true", help="Generate queries + search only")
    parser.add_argument("--crawl-only", action="store_true", help="Crawl + generate leads only")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--instances", type=int, default=3, help="Listing crawler instances")
    parser.add_argument("--min-score", type=float, default=0.2, help="Min lead score")
    parser.add_argument("--json", action="store_true", help="Output JSON summary")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("BPT → GOOGLE MAPS → LEADS PIPELINE")
    log.info("=" * 60)
    
    # Phase 1: Generate search queries from BPT sectors
    if not args.crawl_only:
        log.info("\n--- Phase 1: Generating GMaps search queries from BPT sectors ---")
        sectors = get_sectors()
        if not sectors:
            log.error("No sectors loaded from BPT. Check sectors.yaml.")
            sys.exit(1)
        
        log.info(f"Loaded {len(sectors)} sectors from BPT:")
        for sid, sc in sorted(sectors.items()):
            if sc.get("status") == "active":
                log.info(f"  ✓ {sc.get('display_name', sid)} ({len(sc.get('subsegments', []))} subsegments)")
        
        queries = generate_search_queries(sectors)
        write_queries_file(queries)
        
        report = generate_query_report(queries, sectors)
        log.info(f"\n{report}")
        
        if args.generate_only:
            if args.json:
                print(json.dumps({"queries_generated": len(queries), "sectors": len(sectors)}, indent=2))
            return
    
    # Phase 2: Run search crawler
    if not args.crawl_only:
        log.info("\n--- Phase 2: Running GMaps search crawler ---")
        success = run_search_crawler(dry_run=args.dry_run)
        if not success and not args.dry_run:
            log.warning("Search crawler had issues. Continuing with listing phase...")
    
    # Phase 3: Listing crawl
    log.info("\n--- Phase 3: Listing extraction ---")
    unexported = True
    if not args.dry_run and not args.generate_only:
        url_count = export_uncrawled_urls()
        if url_count > 0:
            log.info(f"Running listing crawler on {url_count} URLs...")
            run_listing_crawler(instances=args.instances)
            unexported = False
        elif url_count == 0:
            log.info("No uncrawled URLs — search may still need to run")
            unexported = False
    else:
        if args.dry_run:
            export_uncrawled_urls(dry_run=True)
    
    # Phase 4: Generate leads by BPT sector
    log.info("\n--- Phase 4: Generating leads by BPT sector ---")
    summary = generate_sector_leads(dry_run=args.dry_run)
    
    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    
    # Print readable summary
    log.info("\n" + "=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info("=" * 60)
    
    total_leads = sum(s.get("count", 0) if isinstance(s, dict) else 0 for s in summary.values())
    log.info(f"Total leads by sector: {total_leads}")
    for sid, data in sorted(summary.items()):
        if isinstance(data, dict):
            log.info(f"  {data.get('display', sid)}: {data.get('count', 0)} leads")
    
    log.info(f"\nLeads saved to: {OUTPUT_DIR / datetime.now(timezone.utc).strftime('%Y-%m-%d')}")


if __name__ == "__main__":
    main()