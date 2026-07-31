from __future__ import annotations

import logging
from pathlib import Path

METHOD_FALLBACK_RULE = "fallback_rule"
METHOD_FALLBACK_LLM_ERROR = "fallback_llm_error"
METHOD_LLM_CACHED = "llm_cached"
METHOD_LLM_PREFIX = "llm_"

BN_STOP = {
    "দোকান", "এজেন্সি", "বাংলাদেশ", "ঢাকা", "সেবা", "কেন্দ্র",
    "কোম্পানি", "অফিস", "কনসাল্টেন্ট", "প্রশিক্ষণ", "পরিষেবা",
    "কারখানা", "প্রতিষ্ঠান", "ভবন", "যত্ন", "সারাইয়ের",
    "রপ্তানিকারক", "প্রস্তুতকর্তা",
}

DEFAULT_SECTOR = "high-roi-niches"

REPO_ROOT = Path(__file__).resolve().parents[1]
BPT_DIR = REPO_ROOT.parent / "business-plan-template"

log = logging.getLogger("classification")


def load_sectors() -> dict:
    import yaml

    path = BPT_DIR / "_system" / "config" / "sectors.yaml"
    if not path.exists():
        log.error(f"sectors.yaml not found at {path}")
        return {}
    data = yaml.safe_load(path.read_text())
    return data.get("sectors", {})


def _single_fallback(lead: dict, index: int, sectors: dict) -> dict:
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

    return {"index": index, "sector": DEFAULT_SECTOR, "confidence": 0.3, "reasoning": "rule-based no match"}