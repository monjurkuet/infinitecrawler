"""scripts/firehose_queries.py — Query-matrix construction for the firehose.

Extracted from scripts/db_linkedin_firehose.py in 2026-08-12 (B3) so the
Python-side query composition is isolated from the worker / circuit-breaker
logic. Templates themselves live in the YAML config; this module just fans
out the role×city×industry cross product into a flat query list.
"""
from __future__ import annotations

import random


def generate_queries(cfg: dict, max_queries: int | None = None) -> list[dict]:
    """Generate flat list of {query, params: {region}, family} dicts."""
    families = cfg["query_families"]
    roles = cfg["roles"]
    locations = cfg["locations"]
    industries = cfg["industries"]
    regions = cfg["regions"]
    queries: list[dict] = []

    if families.get("role_city", {}).get("enabled", True):
        tmpl = families["role_city"]["template"]
        rk = families["role_city"]["region"]
        for role in roles:
            for city in locations["bangladesh"]:
                queries.append({
                    "query": tmpl.format(role=role, city=city),
                    "params": {"region": regions[rk]},
                    "family": "role_city",
                })

    if families.get("role_city_industry", {}).get("enabled", True):
        tmpl = families["role_city_industry"]["template"]
        rk = families["role_city_industry"]["region"]
        for role in roles:
            for city in locations["global"]:
                for industry in industries:
                    queries.append({
                        "query": tmpl.format(role=role, city=city, industry=industry),
                        "params": {"region": regions[rk]},
                        "family": "role_city_industry",
                    })

    if families.get("role_only", {}).get("enabled", True):
        tmpl = families["role_only"]["template"]
        rk = families["role_only"]["region"]
        for role in roles:
            queries.append({
                "query": tmpl.format(role=role),
                "params": {"region": regions[rk]},
                "family": "role_only",
            })

    if max_queries and len(queries) > max_queries:
        queries = random.sample(queries, max_queries)

    return queries
