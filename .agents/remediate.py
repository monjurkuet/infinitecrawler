#!/usr/bin/env python3
"""Self-remediation for knowledge-base.md — discovers current system state
and reports what lines need patching in the prompt document.

Run after Phase 0 finds STALE entries:
    uv run python .agents/remediate.py

Then patch the identified lines in .agents/knowledge-base.md and re-run Phase 0.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

KB = Path(__file__).resolve().parent.parent / '.agents' / 'knowledge-base.md'
if not KB.exists():
    print(f"ERROR: {KB} not found")
    sys.exit(1)

def sh(cmd: str, timeout: int = 15) -> str:
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except Exception:
        return ''

changes = []

# 1. Discover pinchtab config
cfg_path = Path('/root/.pinchtab/config.json')
bridge_port = ''
token = ''
if cfg_path.exists():
    with open(cfg_path) as f:
        pc = json.load(f)
    bridge_port = str(pc.get('bridge', {}).get('port', pc.get('instancePortStart', '9868')))
    token = pc.get('server', {}).get('token', '')
    changes.append(('BRIDGE_PORT', bridge_port))
    changes.append(('TOKEN', token))

# 2. Discover systemd daemon units
units = sh("systemctl --user list-units --no-pager --plain 2>/dev/null | grep infinitecrawler | awk '{print $1}'").split()
if units:
    changes.append(('DAEMON_UNITS', ' '.join(units)))

# 3. Discover active Redis queue namespaces by probing known patterns
KNOWN_NS = ['gmaps_bd_business', 'gmaps']
ns_found = []
for ns in KNOWN_NS:
    ok = sh(f"redis-cli EXISTS {ns}:pending 2>/dev/null")
    if ok:
        ns_found.append(ns)
if ns_found:
    changes.append(('REDIS_PREFIXES', ' '.join(ns_found)))

# 4. Check dead-file assertions
DEAD_FILES = [
    'strategies/input/__init__.py',
    'scripts/check-stuck-chrome.sh',
    'AGENTS.md',
    'docs/GMAPS_LISTINGS_SCRAPER.md',
    'docs/GMAPS_SEARCH_SCRAPER.md',
]
repo_root = KB.parents[1]
reappeared = [f for f in DEAD_FILES if (repo_root / f).exists()]
if reappeared:
    changes.append(('REAPPEARED', ' '.join(reappeared)))

# 5. Discover config YAML files
import glob
configs = sorted(glob.glob(str(repo_root / 'config' / 'gmaps_*.yaml')))
if configs:
    changes.append(('CONFIG_FILES', ' '.join(os.path.basename(c) for c in configs)))

# 6. Verify configs load
try:
    sys.path.insert(0, str(repo_root))
    from factory.scraper_factory import ScraperFactory
    for name in configs:
        ScraperFactory.load_config(name)
    changes.append(('CONFIGS_LOAD', 'OK'))
except Exception as e:
    changes.append(('CONFIGS_LOAD', f'FAIL: {e}'))

print('=== CURRENT STATE ===')
for k, v in changes:
    print(f'  {k}: {v}')

# Build patch instructions
print()
print('=== PATCH INSTRUCTIONS ===')
print('Edit .agents/knowledge-base.md and update:')
print()

need_patch = False

if bridge_port and bridge_port != '9868':
    need_patch = True
    print(f'  - pinchtab bridge port: 9868 -> {bridge_port}')
    print(f'    (in Phase 1 curl command AND KEY FACTS table: pinchtab bridge)')
if token and token != '123456':
    need_patch = True
    print(f'  - pinchtab token: 123456 -> {token}')
    print(f'    (in KEY FACTS table: pinchtab config token)')
if reappeared:
    need_patch = True
    for f in reappeared:
        print(f'  - Remove {f} from dead-file list (file still exists in repo)')

if ns_found and ns_found != KNOWN_NS:
    need_patch = True
    print(f'  - Redis prefixes: {" ".join(KNOWN_NS)} -> {" ".join(ns_found)}')
    print(f'    (in Phase 2 commands AND KEY FACTS table)')

if not need_patch:
    print('  No drift detected. Nothing to patch.')
else:
    print()
    print('1. Use patch() to update each identified line in .agents/knowledge-base.md')
    print('2. Re-run Phase 0 to confirm all STALE entries are resolved')
