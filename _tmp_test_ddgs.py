#!/usr/bin/env python3
"""Test: what does DDGS return for a luxury hotel LinkedIn search?"""
import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx, asyncio

DDGS = "https://search.datasolved.org/search/text"
async def test():
    async with httpx.AsyncClient(timeout=httpx.Timeout(20)) as client:
        queries = [
            'site:linkedin.com/in/ "Radisson Blu Dhaka Water Garden" "CEO"',
            'site:linkedin.com/in/ "InterContinental Dhaka" "Director"',
            'site:linkedin.com/in/ "The Westin Dhaka" Dhaka',
            'site:facebook.com "Radisson Blu Dhaka Water Garden" Dhaka',
        ]
        for q in queries:
            print(f"\nQuery: {q}")
            resp = await client.get(DDGS, params={"query": q, "max_results": 8, "region": "bd-bn"})
            print(f"Status: {resp.status_code}")
            data = resp.json()
            results = data.get("results", [])
            print(f"Results: {len(results)}")
            for r in results[:6]:
                href = r.get("href", "")
                title = r.get("title", "")[:80]
                body = r.get("body", "")[:120]
                has_in = "/in/" in href
                has_fb = "facebook.com" in href
                print(f"  {'[LI]' if has_in else '[  ]'} {'[FB]' if has_fb else '[  ]'} {href[:80]:80s}")
                print(f"         {title}")
            await asyncio.sleep(2)

asyncio.run(test())
