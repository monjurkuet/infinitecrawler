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
    """Load active sectors from BPT config.

    Reads BOTH sectors.yaml (BD-business sectors) AND software_sectors.yaml
    (software product sectors). The search query generator targets buyer
    business types defined in software_sectors.yaml, so the classifier must
    be able to assign listings to those software product sectors too —
    otherwise every garments-factory query result gets misclassified as
    `clothing-fashion` and software product leads disappear.

    Software sectors are namespaced with the `software:` prefix to keep
    them separate from BD-business sector IDs.
    """
    import yaml

    sectors: dict = {}

    # 1) BD-business sectors (clothing-fashion, food-beverage, …)
    bd_path = BPT_DIR / "_system" / "config" / "sectors.yaml"
    if bd_path.exists():
        try:
            bd_data = yaml.safe_load(bd_path.read_text())
            for sid, sc in (bd_data.get("sectors") or {}).items():
                if sc.get("status") == "active":
                    sectors[sid] = sc
        except Exception as e:
            log.error(f"Failed to load {bd_path}: {e}")
    else:
        log.warning(f"sectors.yaml not found at {bd_path}")

    # 2) Software product sectors (payroll, inventory, pos_retail, …)
    sw_path = BPT_DIR / "_system" / "config" / "software_sectors.yaml"
    if sw_path.exists():
        try:
            sw_data = yaml.safe_load(sw_path.read_text())
            for sid, sc in (sw_data.get("sectors") or {}).items():
                if sc.get("status") != "active":
                    continue
                # Map software product keywords → classifier-compatible structure
                # The classifier expects: keywords.en/bn, subsegments, priority_weight
                tbt = sc.get("target_business_types") or {}
                en_targets = tbt.get("en") or []
                bn_targets = tbt.get("bn") or []
                sectors[f"software:{sid}"] = {
                    "display_name": sc.get("display_name") or sid,
                    "product_name": sc.get("product_name") or sid,
                    "keywords": {"en": en_targets, "bn": bn_targets},
                    "subsegments": sc.get("subsegments") or [],
                    "priority_weight": sc.get("priority_weight", 0.5),
                    "status": "active",
                    "_source": "software_sectors",
                }
        except Exception as e:
            log.error(f"Failed to load {sw_path}: {e}")
    else:
        log.warning(f"software_sectors.yaml not found at {sw_path}")

    log.info(
        f"Loaded {len(sectors)} active sectors "
        f"({sum(1 for s in sectors.values() if s.get('_source') == 'software_sectors')} software)"
    )
    return sectors


def _single_fallback(lead: dict, index: int, sectors: dict) -> dict:
    """Rule-based fallback classifier.

    Iteration order matters: software product sectors are searched FIRST so
    that "Garments Factory Ltd" lands on `software:payroll` (the buyer of
    payroll software) instead of `clothing-fashion` (the buyer of fashion
    services). This matches the buyer-facing query design from BPT.
    """
    category = (lead.get("category") or "").lower()
    name = (lead.get("name") or "").lower()

    # Software sectors first — they're buyer-facing, higher signal
    ordered = sorted(
        sectors.items(),
        key=lambda kv: (0 if kv[1].get("_source") == "software_sectors" else 1, kv[0]),
    )

    for sid, sc in ordered:
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