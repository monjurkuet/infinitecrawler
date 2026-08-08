"""utils/transliterate.py — Bengali (and other Indic) script transliteration.

Used by the LinkedIn firehose/search to pre-transliterate BN scripts to Latin
before dispatching to the DDGS upstream (`search.datasolved.org`), which has
been returning HTTP 500 on ~15% of Bengali-script queries.
"""
from __future__ import annotations

import logging
import re
from functools import lru_cache
from typing import Callable

log = logging.getLogger(__name__)

# Bengali Unicode block: U+0980..U+09FF
_BENGALI_RE = re.compile(r"[\u0980-\u09FF]")

# indic_transliteration is heavy (~7MB); import lazily so modules that don't
# need it pay nothing.  We use a Callable alias typed at module load time
# so static analyzers don't flag the import as "possibly unbound" — when the
# dep is missing we substitute a no-op that returns the original string.
try:
    from indic_transliteration import sanscript as _sanscript
    _itx_transliterate: Callable[[str, str, str], str] = _sanscript.transliterate
except ImportError:  # pragma: no cover
    _sanscript = None  # type: ignore[assignment]
    _itx_transliterate = lambda text, _a, _b: text  # noqa: E731


def contains_bengali(text: str) -> bool:
    """True if `text` contains any character in the Bengali Unicode block."""
    return bool(text and _BENGALI_RE.search(text))


@lru_cache(maxsize=4096)
def _transliterate_cached(text: str) -> str:
    if _sanscript is None:
        return text
    try:
        # Devanagari/Bengali → Latin.  Indic-Transliteration defaults to ITRANS;
        # `?` text indicates unmapped chars which we leave in place so brand
        # names stay recognizable rather than becoming garbled output.
        return _itx_transliterate(text, _sanscript.BENGALI, _sanscript.ISO)
    except Exception as exc:  # pragma: no cover
        log.debug("transliterate failed for %r: %s", text[:40], exc)
        return text


def bn_to_en(query: str) -> str:
    """Transliterate a Bengali-script query to Latin.

    Safety: if transliteration yields 0 Latin letters (e.g. brand glyphs the
    library cannot map), the original query is returned unchanged.
    """
    if not contains_bengali(query):
        return query
    out = _transliterate_cached(query)
    if not any(c.isascii() and c.isalpha() for c in out):
        return query
    return out
