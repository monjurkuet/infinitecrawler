#!/usr/bin/env python3
"""LLM-based lead classifier with self-improving few-shot learning.

Classifies GMaps listings into BPT sectors using an LLM.
Stores every classification as a training example for future runs.
Loads best examples as few-shot context on subsequent runs.

Usage:
    # Classify from standard input (JSON array of leads)
    cat leads.json | uv run python scripts/llm_classifier.py

    # Classify and output to file
    uv run python scripts/llm_classifier.py --output results.json

    # Test a few manual leads
    uv run python scripts/llm_classifier.py --test
"""

import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("llm_classifier")

REPO_ROOT = Path(__file__).resolve().parent.parent.parent  # ~/codebase/vhd
INFINITECRAWLER_DIR = REPO_ROOT / "infinitecrawler"
BPT_DIR = REPO_ROOT / "business-plan-template"
CLASSIFICATION_DIR = INFINITECRAWLER_DIR / "_system" / "classification"

# LLM settings
LLM_BASE_URL = os.environ.get(
    "LLM_BASE_URL", "https://llm.datasolved.org/v1"
)
LLM_API_KEY = os.environ.get(
    "LLM_API_KEY", "sk-vqxAskCoUVgzhVybz"
)
LLM_MODEL = os.environ.get(
    "LLM_CLASSIFIER_MODEL", "deepseek-ai/deepseek-v4-flash"
)

BATCH_SIZE = 50  # leads per LLM call (DeepSeek V4 Flash has 1M context)
MAX_FEW_SHOT = 10  # max few-shot examples per batch
MIN_FEW_SHOT_PER_SECTOR = 2  # min examples to keep per sector

# classification_method — single source of truth for the PG column enum.
# Both listing_daemon.py (in-stream fallback) and db_classify.py (offline cron)
# write these values; reporting queries depend on them staying stable.
METHOD_FALLBACK_RULE = "fallback_rule"        # rule-based, in-stream (listing_daemon)
METHOD_FALLBACK_LLM_ERROR = "fallback_llm_error"  # LLM call failed, fell back to rules
METHOD_LLM_CACHED = "llm_cached"              # loaded from training_examples.jsonl
METHOD_LLM_PREFIX = "llm_"                    # prefix for live LLM classifications

# Bengali stop words — common across both rule-based passes; module-level to dedupe.
BN_STOP = {
    "দোকান", "এজেন্সি", "বাংলাদেশ", "ঢাকা", "সেবা", "কেন্দ্র",
    "কোম্পানি", "অফিস", "কনসাল্টেন্ট", "প্রশিক্ষণ", "পরিষেবা",
    "কারখানা", "প্রতিষ্ঠান", "ভবন", "যত্ন", "সারাইয়ের",
    "রপ্তানিকারক", "প্রস্তুতকর্তা",
}


def ensure_dirs() -> None:
    CLASSIFICATION_DIR.mkdir(parents=True, exist_ok=True)


def load_sectors() -> dict:
    """Load BPT sector configs from sectors.yaml."""
    import yaml

    path = BPT_DIR / "_system" / "config" / "sectors.yaml"
    if not path.exists():
        log.error(f"sectors.yaml not found at {path}")
        return {}
    data = yaml.safe_load(path.read_text())
    return data.get("sectors", {})


def build_sector_definitions(sectors: dict) -> dict:
    """Build a clean sector definitions dict from sectors config."""
    definitions = {}
    for sid, sc in sectors.items():
        if sc.get("status") != "active":
            continue
        definitions[sid] = {
            "id": sid,
            "name": sc.get("display_name", sid),
            "keywords": sc.get("keywords", {}),
            "subsegments": sc.get("subsegments", []),
            "description": _generate_description(sid, sc),
        }
    return definitions


def _generate_description(sid: str, sc: dict) -> str:
    """Generate a natural language description of a sector."""
    name = sc.get("display_name", sid)
    keywords = []
    kw_dict = sc.get("keywords", {})
    keywords.extend(kw_dict.get("en", []))
    keywords_str = ", ".join(keywords[:6])
    subs = sc.get("subsegments", [])
    subs_str = ", ".join(subs[:4])
    parts = [f"{name} — businesses related to"]
    if keywords_str:
        parts.append(f"keywords like: {keywords_str}")
    if subs_str:
        parts.append(f"sub-verticals: {subs_str}")
    return ". ".join(parts)


def load_training_examples() -> list[dict]:
    """Load saved training examples, deduplicated by (name, website)."""
    path = CLASSIFICATION_DIR / "training_examples.jsonl"
    if not path.exists():
        return []
    examples = []
    seen = set()
    for line in path.read_text().strip().splitlines():
        if not line.strip():
            continue
        try:
            ex = json.loads(line)
            key = (ex.get("name", ""), ex.get("website", ""))
            if key not in seen:
                seen.add(key)
                examples.append(ex)
        except json.JSONDecodeError:
            continue
    log.info(f"Loaded {len(examples)} unique training examples")
    return examples


def save_training_examples(examples: list[dict]) -> None:
    """Append new training examples to the JSONL file."""
    path = CLASSIFICATION_DIR / "training_examples.jsonl"
    with open(path, "a", encoding="utf-8") as f:
        for ex in examples:
            f.write(json.dumps(ex, ensure_ascii=False) + "\n")
    log.info(f"Saved {len(examples)} new training examples to {path}")


def select_few_shot(examples: list[dict], sectors: dict, max_count: int = MAX_FEW_SHOT) -> list[dict]:
    """Select best few-shot examples: highest confidence, balanced across sectors."""
    active_sectors = {s for s, c in sectors.items() if c.get("status") == "active"}

    # Group by sector
    by_sector: dict[str, list[dict]] = {}
    for ex in examples:
        sec = ex.get("sector", "")
        if sec in active_sectors and ex.get("confidence", 0) >= 0.7:
            by_sector.setdefault(sec, []).append(ex)

    # Sort each group by confidence descending
    for sec in by_sector:
        by_sector[sec].sort(key=lambda x: -x.get("confidence", 0))

    # Take top N per sector
    selected = []
    # First pass: take the best from each sector
    for sec in sorted(active_sectors):
        pool = by_sector.get(sec, [])
        selected.extend(pool[:MIN_FEW_SHOT_PER_SECTOR])

    # Second pass: fill remaining slots with best remaining
    remaining = max_count - len(selected)
    if remaining > 0:
        more = []
        for sec in sorted(active_sectors):
            pool = by_sector.get(sec, [])
            more.extend(pool[MIN_FEW_SHOT_PER_SECTOR:])
        # Sort remaining by confidence
        more.sort(key=lambda x: -x.get("confidence", 0))
        selected.extend(more[:remaining])

    # Shuffle to avoid positional bias
    random.shuffle(selected)
    log.info(f"Selected {len(selected)} few-shot examples ({len(by_sector)} sectors represented)")
    return selected


def format_sector_definitions(definitions: dict) -> str:
    """Format sector definitions for the prompt — full descriptions (1M context)."""
    lines = []
    for sid, sd in sorted(definitions.items()):
        lines.append(f"  - `{sid}`: {sd['name']} — {sd['description']}")
    return "\n".join(lines)


def format_few_shot(examples: list[dict]) -> str:
    """Format few-shot examples for the prompt."""
    lines = []
    for i, ex in enumerate(examples[:MAX_FEW_SHOT]):
        lines.append(
            f"  {i}. Business: \"{ex.get('name', '')}\" | "
            f"Category: \"{ex.get('category', '')}\" | "
            f"Website: {ex.get('website', '')} → "
            f"Sector: `{ex.get('sector', '')}` "
            f"(confidence: {ex.get('confidence', 0):.2f})"
        )
        if ex.get("reasoning"):
            lines.append(f"     Reason: {ex['reasoning']}")
    return "\n".join(lines)


def format_leads_batch(leads: list[dict], start_index: int) -> str:
    """Format a batch of leads for classification."""
    lines = []
    for i, lead in enumerate(leads):
        idx = start_index + i
        name = lead.get("name", "")
        cat = lead.get("category", "")
        web = lead.get("website", "")
        addr = lead.get("address", "")
        rating = lead.get("rating")
        reviews = lead.get("review_count")
        # Rating/reviews add social-proof context: a 4.8★ computer store
        # vs a 3.2★ repair shop disambiguate sectors better than name alone.
        rating_str = (
            f" | Rating: {float(rating):.1f}/5 ({reviews} reviews)"
            if rating is not None and reviews is not None
            else " | Rating: N/A"
        )
        lines.append(
            f"  {idx}. Name: \"{name}\" | Category: \"{cat}\"{rating_str} "
            f"| Website: {web} | Address: \"{addr}\""
        )
    return "\n".join(lines)


def build_classification_prompt(
    sector_definitions: dict,
    few_shot: list[dict],
    leads_batch: list[dict],
    start_index: int,
) -> list[dict]:
    """Build the LLM prompt for a batch of leads."""
    system = f"""You are a business classification AI. Your task is to classify each business listing into exactly one BPT sector.

AVAILABLE SECTORS:
{format_sector_definitions(sector_definitions)}

RULES:
- Each business must be classified into ONE sector only
- If no sector matches well, assign "high-roi-niches" (the catch-all)
- For BIM-related businesses (BIM, Revit, MEP, architectural modeling, scan-to-BIM), assign "bim-global-outreach"
- Consider business name, category, website, and address together
- Return confidence between 0.0 (uncertain) and 1.0 (certain)
- Response MUST be valid JSON only, no other text

FEW-SHOT EXAMPLES (learn from these):
{format_few_shot(few_shot) if few_shot else "No past examples available."}"""

    user = f"""Classify these businesses into the sectors above.

{format_leads_batch(leads_batch, start_index)}

Return JSON: {{"classifications": [
  {{"index": {start_index}, "sector": "sector-id", "confidence": 0.95, "reasoning": "..."}},
  ...
]}}"""

    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def call_llm(messages: list[dict], retries: int = 2, model: str | None = None) -> dict | None:
    """Call the LLM API and return parsed JSON."""
    model = model or LLM_MODEL
    headers = {
        "Authorization": f"Bearer {LLM_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 8192,
        "response_format": {"type": "json_object"},
    }

    for attempt in range(retries):
        try:
            with httpx.Client(timeout=30) as client:
                resp = client.post(
                    f"{LLM_BASE_URL}/chat/completions",
                    headers=headers,
                    json=payload,
                )
                resp.raise_for_status()
                # Handle multi-line JSON responses (some proxies return one object per line)
                resp_text = resp.text.strip()
                body = None
                for line in resp_text.split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        candidate = json.loads(line)
                        if "choices" in candidate:
                            body = candidate
                            break
                    except json.JSONDecodeError:
                        continue
                if body is None:
                    body = json.loads(resp_text.split("\n")[0])
                content = body["choices"][0]["message"]["content"]
                return json.loads(content)
        except httpx.TimeoutException:
            log.warning(f"LLM timeout (attempt {attempt + 1}/{retries})")
            if attempt < retries - 1:
                time.sleep(3)
        except httpx.HTTPStatusError as e:
            log.warning(f"LLM HTTP {e.response.status_code} (attempt {attempt + 1}/{retries})")
            if e.response.status_code == 413:
                log.error("Payload too large — reducing batch size may help")
                return None  # Don't retry 413 — it won't help
            if attempt < retries - 1:
                sleep_time = 5 * (attempt + 1)  # 5s, 10s backoff
                time.sleep(sleep_time)
        except (json.JSONDecodeError, KeyError) as e:
            log.error(f"LLM response parse error: {e}")
            return None
    log.error("LLM call failed after all retries")
    return None


def classify_batch(
    leads: list[dict],
    start_index: int,
    sector_definitions: dict,
    few_shot: list[dict],
    model: str | None = None,
) -> list[dict] | None:
    """Classify a single batch of leads."""
    messages = build_classification_prompt(
        sector_definitions, few_shot, leads, start_index
    )
    result = call_llm(messages, model=model)
    if not result:
        return None

    classifications = result.get("classifications", [])
    log.info(
        f"Batch {start_index}-{start_index + len(leads) - 1}: "
        f"got {len(classifications)} classifications"
    )
    return classifications


def build_lead_snapshot(lead: dict, sector: str, confidence: float, reasoning: str) -> dict:
    """Build a training example from a classification result."""
    return {
        "name": lead.get("name", ""),
        "category": lead.get("category", ""),
        "website": lead.get("website", ""),
        "address": lead.get("address", ""),
        "sector": sector,
        "confidence": round(confidence, 3),
        "reasoning": reasoning,
        "classified_at": datetime.now(timezone.utc).isoformat(),
    }


def classify_all(
    leads: list[dict],
    sectors: dict,
    existing_examples: list[dict],
    dry_run: bool = False,
    model: str | None = None,
    max_leads: int = 0,
) -> list[dict]:
    """Classify leads into BPT sectors using LLM.

    Processes in batches, saving training examples after each batch.
    Classifies the first `max_leads` via LLM, falls back to rule-based for the rest.
    Set max_leads=0 to classify all.

    Returns list of dicts with keys: index, sector, confidence, reasoning.
    """
    sector_defs = build_sector_definitions(sectors)
    few_shot = select_few_shot(existing_examples, sectors)

    # Dedup already-classified leads (by name+website)
    classified_keys = set()
    for ex in existing_examples:
        key = (ex.get("name", ""), ex.get("website", ""))
        classified_keys.add(key)

    all_results: list[dict] = []
    new_examples: list[dict] = []

    # Split: LLM-classifiable vs fallback
    llm_leads = []
    fallback_indices = []
    already_have = 0
    for i, lead in enumerate(leads):
        key = (lead.get("name", ""), lead.get("website", ""))
        if key in classified_keys:
            already_have += 1
            # Load stored classification from training examples
            stored = next(
                (ex for ex in existing_examples
                 if ex.get("name") == lead.get("name")
                 and ex.get("website") == lead.get("website")),
                None,
            )
            if stored:
                all_results.append({
                    "index": i,
                    "sector": stored["sector"],
                    "confidence": stored.get("confidence", 0.95),
                    "reasoning": stored.get("reasoning", "from training examples"),
                })
            else:
                fallback_indices.append((i, lead))
        elif max_leads > 0 and len(llm_leads) >= max_leads:
            fallback_indices.append((i, lead))
        else:
            llm_leads.append((i, lead))

    log.info(
        f"Already classified: {already_have}, "
        f"LLM batch: {len(llm_leads)}, "
        f"Fallback: {len(fallback_indices)}"
    )

    # Process LLM-classifiable leads in batches
    for batch_start in range(0, len(llm_leads), BATCH_SIZE):
        batch = llm_leads[batch_start:batch_start + BATCH_SIZE]
        batch_indices = [item[0] for item in batch]
        batch_leads = [item[1] for item in batch]

        log.info(
            f"LLM batch {batch_start}-{batch_start + len(batch) - 1} "
            f"({len(batch)} leads)..."
        )

        if dry_run:
            log.info(f"  [DRY-RUN] Would classify {len(batch)} leads")
            # Still produce results for dry-run — use fallback
            for idx, lead in batch:
                result = _single_fallback(lead, idx, sectors)
                all_results.append(result)
            continue

        results = classify_batch(
            batch_leads, min(batch_indices), sector_defs, few_shot, model=model
        )
        if results is None:
            log.warning(f"LLM failed for batch starting at {batch_start}, using fallback")
            results = []
            for idx, lead in batch:
                results.append({
                    "index": idx,
                    "sector": "high-roi-niches",
                    "confidence": 0.3,
                    "reasoning": "LLM failed, fallback",
                })
            all_results.extend(results)
            continue

        all_results.extend(results)

        # Save training examples after EACH batch
        batch_new = []
        for r in results:
            rel_idx = r.get("index", 0) - min(batch_indices)
            if 0 <= rel_idx < len(batch_leads):
                conf = r.get("confidence", 0)
                if conf >= 0.7 and r.get("sector"):
                    batch_new.append(
                        build_lead_snapshot(
                            batch_leads[rel_idx],
                            r["sector"],
                            conf,
                            r.get("reasoning", ""),
                        )
                    )
        if batch_new and not dry_run:
            save_training_examples(batch_new)
            new_examples.extend(batch_new)

    # Fallback for non-LLM leads
    for idx, lead in fallback_indices:
        result = _single_fallback(lead, idx, sectors)
        all_results.append(result)

    log.info(
        f"Classification complete: {len(all_results)} results, "
        f"{len(new_examples)} new training examples this run"
    )
    return all_results


def _single_fallback(lead: dict, index: int, sectors: dict) -> dict:
    """Classify a single lead using rule-based fallback."""
    category = (lead.get("category") or "").lower()
    name = (lead.get("name") or "").lower()

    for sid, sc in sorted(sectors.items()):
        if sc.get("status") != "active":
            continue
        kw_dict = sc.get("keywords", {})
        all_keywords = kw_dict.get("en", []) + kw_dict.get("bn", [])
        subsegments = sc.get("subsegments", [])

        # Pass 1
        for kw in all_keywords:
            kw_lower = kw.lower().strip()
            if len(kw_lower) >= 8 and (kw_lower in category or kw_lower in name):
                return {"index": index, "sector": sid, "confidence": 0.85, "reasoning": "rule-based pass 1"}

        # Pass 2
        for sub in subsegments:
            if sub.lower().strip() in category:
                return {"index": index, "sector": sid, "confidence": 0.75, "reasoning": "rule-based pass 2"}

        # Pass 3
        for kw in all_keywords:
            parts = [p for p in kw.lower().split() if len(p) > 4]
            if parts and any(part in name for part in parts):
                return {"index": index, "sector": sid, "confidence": 0.65, "reasoning": "rule-based pass 3"}

        # Pass 4
        for kw in all_keywords:
            bn_words = [
                w for w in kw.lower().split()
                if any("\u0980" <= c <= "\u09FF" for c in w)
                and w not in BN_STOP
            ]
            if bn_words and all(w in category for w in bn_words):
                return {"index": index, "sector": sid, "confidence": 0.60, "reasoning": "rule-based pass 4"}

    return {"index": index, "sector": "high-roi-niches", "confidence": 0.3, "reasoning": "rule-based no match"}


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="LLM-based lead classifier"
    )
    parser.add_argument(
        "--input", "-i",
        type=str,
        help="Input JSON file (array of leads). Reads from stdin if omitted.",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        help="Output file for results (JSON). Prints to stdout if omitted.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Load data and show counts but don't call LLM",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run a small test with manual leads",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=LLM_MODEL,
        help="LLM model to use",
    )
    args = parser.parse_args()

    ensure_dirs()

    # Load sectors
    sectors = load_sectors()
    active = {
        s: c for s, c in sectors.items()
        if c.get("status") == "active"
    }
    log.info(f"Loaded {len(active)} active sectors")

    if args.test:
        # Test mode: classify a few manual leads
        test_leads = [
            {
                "name": "RM Computer",
                "category": "কম্পিউটারের দোকান",
                "website": "https://rmcomputerbd.com/",
                "address": "Khulna",
            },
            {
                "name": "TOKYO LEATHER TECH",
                "category": "চামড়াজাত পণ্য প্রস্তুতকারক",
                "website": "https://tokyoleathertech.com/",
                "address": "Tongi, Dhaka",
            },
            {
                "name": "BIM Engineers Ltd",
                "category": "Architecture & Engineering",
                "website": "https://bimengineers.com/",
                "address": "Dhaka",
            },
            {
                "name": "AL-NUR AUTO HOUSE",
                "category": "পাইকারি",
                "website": "https://www.facebook.com/alnurautohousefeni",
                "address": "Feni",
            },
            {
                "name": "Advance Care Specialized Hospital",
                "category": "হাসপাতাল",
                "website": "https://acspecializedhospital.blogspot.com/",
                "address": "Narayanganj",
            },
        ]
        log.info(f"Test mode: {len(test_leads)} leads")

        existing = load_training_examples()
        results = classify_all(test_leads, sectors, existing, dry_run=args.dry_run, model=args.model)

        print("\n=== CLASSIFICATION RESULTS ===")
        for r in results:
            idx = r["index"]
            lead = test_leads[idx]
            print(
                f"  [{idx}] {lead['name']}: {r['sector']} "
                f"(conf={r['confidence']:.2f}) — {r.get('reasoning', '')[:80]}"
            )
        return

    # Load leads
    if args.input:
        leads = json.loads(Path(args.input).read_text(encoding="utf-8"))
    else:
        leads = json.loads(sys.stdin.read())

    log.info(f"Loaded {len(leads)} leads for classification")

    if not leads:
        print(json.dumps({"classifications": []}))
        return

    existing = load_training_examples()
    results = classify_all(leads, sectors, existing, dry_run=args.dry_run, model=args.model)

    output = {"classifications": results}
    if args.output:
        Path(args.output).write_text(
            json.dumps(output, indent=2, ensure_ascii=False)
        )
        log.info(f"Results written to {args.output}")
    else:
        print(json.dumps(output, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
