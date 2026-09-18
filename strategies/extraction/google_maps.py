from base.strategies import ExtractionStrategy
import logging
import re
from typing import Dict, List


class GoogleMapsExtractionStrategy(ExtractionStrategy):
    """Google Maps-specific extraction using a single JS evaluate call.

    Google Maps result cards contain rich data (name, rating, reviews,
    category, address, hours, phone, website) spread across sibling
    elements within each card. The generic_selector strategy can only
    extract attributes from a single CSS-matched element, so this
    strategy uses a single JavaScript evaluation to extract all fields
    from every result card in the feed.

    Config keys (under ``selectors``):
    items: CSS selector for result link elements (default: ``a.hfpxzc``)
    """

    # Regex to parse "4.8 stars 31 Reviews" or "4.8 stars, 31 reviews"
    _RATING_RE = re.compile(r"([\d.]+)\s*stars?\s*,?\s*(\d[\d,]*)\s*reviews?", re.I)

    # JS extraction snippet — uses single evaluate() to minimise CDP
    # round-trips on the heavy Google Maps DOM.
    _JS_TEMPLATE = """
    (() => {{
        const feed = document.querySelector('div[role="feed"]');
        if (!feed) return [];

        const results = [];
        for (const card of feed.children) {{
            const link = card.querySelector('{selector}');
            if (!link) continue;

            const name = link.getAttribute('aria-label') || '';
            const url = link.href || '';
            const dataCid = link.getAttribute('data-cid') || '';

            // Rating from role="img"
            const ratingEl = card.querySelector('[role="img"]');
            const ratingStr = ratingEl?.getAttribute('aria-label') || '';

            // Phone
            const phoneLink = card.querySelector('a[href^="tel:"]');
            const phone = phoneLink?.textContent?.trim() || '';

            // Website
            const websiteLink = card.querySelector('a[data-tooltip="Open website"]');
            const website = websiteLink?.href || '';

            // Parse structured text from spans
            const spans = [...card.querySelectorAll('span')]
                .map(s => s.textContent?.trim())
                .filter(Boolean);

            // Find category, address, hours from span text
            const seen = new Set();
            let category = '';
            let address = '';
            let hours = '';
            for (const txt of spans) {{
                if (seen.has(txt)) continue;
                seen.add(txt);

                // Skip rating numbers like "4.8", "4.8(31)"
                if (/^[\\d.]+(\\(\\d+\\))?$/.test(txt)) continue;
                // Skip standalone review counts like "(31)"
                if (/^\\(\\d+\\)$/.test(txt)) continue;
                // Skip "No reviews" placeholder
                if (/^no reviews$/i.test(txt)) continue;

                // Hours: contains open/closed/closes (check BEFORE category/address)
                if (!hours && /opens?\\s|closes?\\s|closed|open\\s|temporarily closed/i.test(txt)) {{
                    hours = txt;
                    continue;
                }}

                // Category: short text, no leading dot, not hours, not starting with digit
                if (!category && txt.length < 60
                    && !txt.startsWith('\\u00b7')
                    && !/^\\d/.test(txt)) {{
                    category = txt;
                    continue;
                }}

                // Address: starts with dot separator (but not hours) or looks like a street address
                if (!address && (
                    (txt.startsWith('\\u00b7') && !/closes?\\s|opens?\\s|closed/i.test(txt)) ||
                    (/\\d+\\s+\\w+.*(road|street|ave|blvd|lane|rd|st|colony|floor|house|block)/i.test(txt)
                     && !/closes?\\s|opens?\\s|closed/i.test(txt))
                )) {{
                    address = txt.replace(/^[\\s\\u00b7]+/, '').trim();
                    continue;
                }}
            }}

            results.push({{
                name: name,
                url: url,
                data_cid: dataCid,
                rating_str: ratingStr,
                phone: phone,
                website: website,
                category: category,
                address: address,
                hours: hours,
            }});
        }}
        return results;
    }})()
    """

    def __init__(self, browser_manager, config: dict):
        super().__init__(browser_manager, config)
        self._seen_cids = set()

    async def extract_items(self) -> List[Dict]:
        """Extract all result items from the current Google Maps page."""
        try:
            tab = self.browser_manager.tab
            if not tab:
                self.logger.error("No tab available for extraction")
                return []

            selectors_config = self.config.get("selectors", {})
            items_selector = selectors_config.get("items", "a.hfpxzc")

            escaped = items_selector.replace("'", "\\'")
            js = self._JS_TEMPLATE.format(selector=escaped)

            raw_items = await tab.evaluate(js)

            if not raw_items:
                self.logger.warning("JS extraction returned no items")
                return []

            # Normalise CDP RemoteObject serialisation into plain dicts
            items = self._normalise(raw_items)

            # Parse rating / reviews from the rating string
            for item in items:
                rs = item.get("rating_str", "")
                m = self._RATING_RE.match(rs)
                if m:
                    item["rating"] = float(m.group(1))
                    item["reviews"] = int(m.group(2).replace(",", ""))
                # Remove the raw string to keep output clean
                item.pop("rating_str", None)

            # Deduplicate by data_cid or url
            deduped = []
            for item in items:
                key = item.get("data_cid") or item.get("url") or item.get("name")
                if key and key in self._seen_cids:
                    continue
                if key:
                    self._seen_cids.add(key)
                deduped.append(item)

            self.logger.info(f"Extracted {len(deduped)} items ({len(items)} raw)")
            return self._map_to_schema(deduped)

        except Exception as e:
            self.logger.error(f"Error extracting items: {e}")
            return []

    def _normalise(self, raw_items) -> List[Dict]:
        """Normalise CDP evaluation result into a list of plain dicts.

        nodriver's tab.evaluate() returns CDP RemoteObject values. When the
        JS returns an array of objects, each object may be serialised as
        ``[[key, {type, value}], ...]`` instead of ``{key: value, ...}``.
        This method converts to plain dicts.
        """
        if not raw_items:
            return []

        # Unwrap top-level CDP RemoteObject wrapper
        # e.g. {"type": "object", "value": [...]}
        if isinstance(raw_items, dict) and "type" in raw_items and "value" in raw_items:
            raw_items = raw_items["value"]

        if not isinstance(raw_items, list) or len(raw_items) == 0:
            return []

        first = raw_items[0]

        # Already plain dicts?
        if isinstance(first, dict) and "type" not in first:
            return raw_items

        # CDP format: each entry is {"type": "object", "value": [[key, {type, value}], ...]}
        result = []
        for entry in raw_items:
            # Unwrap per-entry CDP wrapper
            if isinstance(entry, dict) and "type" in entry and "value" in entry:
                entry = entry["value"]

            if isinstance(entry, list):
                d = {}
                for pair in entry:
                    if (
                        isinstance(pair, list)
                        and len(pair) == 2
                        and isinstance(pair[1], dict)
                        and "value" in pair[1]
                    ):
                        d[pair[0]] = pair[1]["value"]
                    elif (
                        isinstance(pair, (list, tuple))
                        and len(pair) >= 2
                    ):
                        d[pair[0]] = pair[1]
                result.append(d)
            elif isinstance(entry, dict):
                result.append(entry)

        return result

    def _map_to_schema(self, items: List[Dict]) -> List[Dict]:
        """Map extraction field names to PostgreSQL schema field names.

        The JavaScript extraction uses short names (url, reviews, data_cid)
        but the PostgreSQL listing_details table expects (source_url,
        review_count, place_id). This mapping bridges the two.
        """
        mapped = []
        for item in items:
            row = dict(item)
            # url → source_url
            if "url" in row and "source_url" not in row:
                row["source_url"] = row.pop("url")
            # data_cid → place_id
            if "data_cid" in row and "place_id" not in row:
                row["place_id"] = row.pop("data_cid")
            # reviews → review_count
            if "reviews" in row and "review_count" not in row:
                row["review_count"] = row.pop("reviews")
            mapped.append(row)
        return mapped
