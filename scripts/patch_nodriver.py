#!/usr/bin/env python3
"""
Post-install patcher for nodriver 0.48.1.

Fixes upstream bugs that break headless scraping on VPS:

1. Browser startup timeout too short (5x0.25s ~ 1.25s).
   Increased to 20x1.0s so Chrome has time to open its
   debug port on slow/loaded systems.

2. filter_recurse_all crashes on empty shadow_roots list.
   Guard: `is not None` catches empty lists -> IndexError.
   Changed to truthy check (empty list = falsy).

3. query_selector_all silently drops nodes that filter_recurse
   can't find in the cached DOM tree (deep/shadow DOM).
   Fallback: use cdp.dom.describe_node to retrieve a missing
   node directly from the browser instead of skipping it.

Run after `uv sync` via:
  ./sync.sh        (recommended -- wraps uv sync + patch)
  make sync        (equivalent)
  uv run python scripts/patch_nodriver.py  (manual)
"""

import sys
from pathlib import Path

# --- Locate nodriver package -----------------------------------------------
try:
    import nodriver
    NODRIVER_DIR = Path(nodriver.__file__).parent
except ImportError:
    print("ERROR: nodriver is not installed. Run `uv sync` first.", file=sys.stderr)
    sys.exit(1)

VERSION = getattr(nodriver, "__version__", getattr(nodriver, "version", "unknown"))
print(f"Patching nodriver {VERSION} at {NODRIVER_DIR}")


# --- Helpers ----------------------------------------------------------------

def read(path: Path) -> str:
    """Read file and normalize CRLF -> LF for reliable pattern matching."""
    return path.read_text(encoding="utf-8").replace("\r\n", "\n")


def write(path: Path, content: str) -> None:
    """Write file, preserving original line ending style (CRLF or LF)."""
    raw = path.read_bytes()
    uses_crlf = b"\r\n" in raw[:2000]
    if uses_crlf:
        content = content.replace("\n", "\r\n")
    path.write_text(content, encoding="utf-8")


def apply_patch(path: Path, label: str, old: str, new: str) -> bool:
    """Replace `old` with `new` in file. Idempotent. Returns True if patched."""
    content = read(path)
    if old in content:
        content = content.replace(old, new, 1)
        write(path, content)
        print(f"  [OK]   {label}")
        return True
    if new in content:
        print(f"  [SKIP] {label} -- already patched")
        return False
    print(f"  [WARN] {label} -- pattern not found")
    return False


# ---------------------------------------------------------------------------
# Patch 1: Browser startup timeout
# File: nodriver/core/browser.py  (LF line endings, spaces indentation)
# ---------------------------------------------------------------------------
browser_py = NODRIVER_DIR / "core" / "browser.py"
if browser_py.exists():
    # The initial sleep + loop start
    # Stock has:     await asyncio.sleep(0.25)\n        for _ in range(5):
    # We want:  await asyncio.sleep(1.0)\n        for _ in range(20):
    apply_patch(
        browser_py,
        "browser startup sleep+loop (0.25s/range(5) -> 1.0s/range(20))",
        old="await asyncio.sleep(0.25)\n        for _ in range(5):",
        new="await asyncio.sleep(1.0)\n        for _ in range(20):",
    )

    # The last-retry index check (stock: _ == 4, target: _ == 19)
    content = read(browser_py)
    if "if _ == 4:" in content:
        apply_patch(
            browser_py,
            "browser last-retry index (4 -> 19)",
            old="if _ == 4:",
            new="if _ == 19:",
        )
    else:
        print("  [SKIP] browser last-retry index -- already 19")

    # The retry sleep between attempts (stock: 0.5s, target: 1.0s)
    content = read(browser_py)
    if "await asyncio.sleep(0.5)" in content:
        apply_patch(
            browser_py,
            "browser retry sleep (0.5s -> 1.0s)",
            old='logger.debug("could not start", exc_info=True)\n            await asyncio.sleep(0.5)',
            new='logger.debug("could not start", exc_info=True)\n            await asyncio.sleep(1.0)',
        )
    else:
        print("  [SKIP] browser retry sleep -- already 1.0s")

    # Verify final state
    content = read(browser_py)
    assert "for _ in range(20):" in content, "browser.py: range(20) not found after patching"
    print("  [VERIFIED] browser startup timeout patched correctly")
else:
    print("  [SKIP] browser.py not found")


# ---------------------------------------------------------------------------
# Patch 2: filter_recurse_all shadow root crash
# File: nodriver/core/util.py  (CRLF line endings, spaces indentation)
#
# util.py uses 12-space indent for the if-block, 16-space for body.
# We build match strings using explicit space counts to avoid
# whitespace collapse in string literals.
# ---------------------------------------------------------------------------
util_py = NODRIVER_DIR / "core" / "util.py"
if util_py.exists():
    # filter_recurse_all: shadow_roots guard
    # 12 spaces before 'if', 16 spaces before 'out.extend'
    INDENT_12 = " " * 12
    INDENT_16 = " " * 16

    apply_patch(
        util_py,
        "filter_recurse_all shadow_roots guard (is not None -> truthy)",
        old=f"{INDENT_12}if child.shadow_roots is not None:\n{INDENT_16}out.extend(filter_recurse_all(child.shadow_roots[0], predicate))",
        new=f"{INDENT_12}if child.shadow_roots:\n{INDENT_16}out.extend(filter_recurse_all(child.shadow_roots[0], predicate))",
    )

    # filter_recurse: add try/except around shadow_roots[0] access
    INDENT_20 = " " * 20
    INDENT_24 = " " * 24

    apply_patch(
        util_py,
        "filter_recurse shadow_roots IndexError guard",
        old=(
            f"{INDENT_12}if child.shadow_roots:\n"
            f"{INDENT_16}shadow_root_result = filter_recurse(child.shadow_roots[0], predicate)"
        ),
        new=(
            f"{INDENT_12}if child.shadow_roots:\n"
            f"{INDENT_16}try:\n"
            f"{INDENT_20}shadow_root_result = filter_recurse(child.shadow_roots[0], predicate)\n"
            f"{INDENT_16}except (IndexError, TypeError):\n"
            f"{INDENT_20}shadow_root_result = None"
        ),
    )
else:
    print("  [SKIP] util.py not found")


# ---------------------------------------------------------------------------
# Patch 3: query_selector_all fallback for missing nodes
# File: nodriver/core/tab.py  (CRLF line endings, spaces indentation)
# ---------------------------------------------------------------------------
tab_py = NODRIVER_DIR / "core" / "tab.py"
if tab_py.exists():
    # tab.py uses 8-space indent for for-loop, 12-space for body
    # (different from util.py which uses 12/16)
    INDENT_8 = " " * 8
    INDENT_12 = " " * 12
    INDENT_16 = " " * 16
    INDENT_20 = " " * 20

    old = (
        f"{INDENT_8}for nid in node_ids:\n"
        f"{INDENT_12}node = util.filter_recurse(doc, lambda n: n.node_id == nid)\n"
        f"{INDENT_12}# we pass along the retrieved document tree,\n"
        f"{INDENT_12}# to improve performance\n"
        f"{INDENT_12}if not node:\n"
        f"{INDENT_16}continue\n"
        f"{INDENT_12}elem = element.create(node, self, doc)\n"
        f"{INDENT_12}items.append(elem)"
    )
    new = (
        f"{INDENT_8}for nid in node_ids:\n"
        f"{INDENT_12}node = util.filter_recurse(doc, lambda n: n.node_id == nid)\n"
        f"{INDENT_12}# we pass along the retrieved document tree,\n"
        f"{INDENT_12}# to improve performance\n"
        f"{INDENT_12}if not node:\n"
        f"{INDENT_16}# Fallback: fetch node directly from browser when\n"
        f"{INDENT_16}# cached DOM tree traversal misses it (deep/shadow DOM)\n"
        f"{INDENT_16}try:\n"
        f"{INDENT_20}desc = await self.send(cdp.dom.describe_node(node_id=nid))\n"
        f"{INDENT_20}node = desc\n"
        f"{INDENT_16}except Exception:\n"
        f"{INDENT_20}continue\n"
        f"{INDENT_16}if not node:\n"
        f"{INDENT_20}continue\n"
        f"{INDENT_12}elem = element.create(node, self, doc)\n"
        f"{INDENT_12}items.append(elem)"
    )

    content = read(tab_py)
    if old in content:
        content = content.replace(old, new, 1)
        write(tab_py, content)
        print("  [OK]   query_selector_all fallback (cdp.dom.describe_node)")
    elif new in content:
        print("  [SKIP] query_selector_all fallback -- already patched")
    else:
        print("  [WARN] query_selector_all -- pattern not found")
else:
    print(" [SKIP] tab.py not found")


# ---------------------------------------------------------------------------
# Patch 4: websockets connect timeout
# File: nodriver/core/connection.py
#
# Stock nodriver uses websockets.connect() without open_timeout,
# defaulting to 10s. On slow/loaded VPS systems, the CDP WebSocket
# handshake to a new tab target can exceed 10s, causing
# "timed out during opening handshake" errors.
# ---------------------------------------------------------------------------
connection_py = NODRIVER_DIR / "core" / "connection.py"
if connection_py.exists():
    apply_patch(
        connection_py,
        "websockets open_timeout (default 10s -> 60s)",
        old="ping_timeout=PING_TIMEOUT,\n                    max_size=MAX_SIZE,",
        new="ping_timeout=PING_TIMEOUT,\n                    open_timeout=60,\n                    max_size=MAX_SIZE,",
    )
else:
    print(" [SKIP] connection.py not found")


print("\nDone. All patches are idempotent -- safe to re-run after `uv sync`.")
