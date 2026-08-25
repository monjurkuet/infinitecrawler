#!/usr/bin/env python3
"""Test OSM website-filtered queries across 29 cities.
Uses single-tag queries (shop+website, office+website) — lighter than 6-way union."""
import json
import subprocess
import time

MIRRORS = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
]

CITIES = [
    ("Dhaka",       (23.64, 90.32, 23.90, 90.56)),
    ("Chittagong",  (22.16, 91.78, 22.44, 91.88)),
    ("Sylhet",      (24.84, 91.83, 24.93, 91.93)),
    ("Rajshahi",    (24.32, 88.52, 24.42, 88.67)),
    ("Khulna",      (22.77, 89.49, 22.89, 89.60)),
    ("Barisal",     (22.68, 90.33, 22.76, 90.41)),
    ("Rangpur",     (25.70, 89.19, 25.79, 89.28)),
    ("Mymensingh",  (24.69, 90.37, 24.78, 90.44)),
    ("Comilla",     (23.41, 91.15, 23.49, 91.22)),
    ("Narayanganj", (23.58, 90.47, 23.64, 90.53)),
    ("Gazipur",     (23.96, 90.37, 24.06, 90.45)),
    ("Nawabganj",   (24.60, 88.35, 24.70, 88.47)),
    ("Dinajpur",    (25.61, 88.61, 25.69, 88.69)),
    ("Tangail",     (24.20, 89.89, 24.28, 89.96)),
    ("Bogra",       (24.78, 89.32, 24.88, 89.40)),
    ("Kolkata",     (22.45, 88.25, 22.70, 88.45)),
    ("Delhi",       (28.38, 76.83, 28.78, 77.35)),
    ("Mumbai",      (18.89, 72.75, 19.30, 72.98)),
    ("Chennai",     (12.90, 80.12, 13.12, 80.31)),
    ("Bangalore",   (12.87, 77.52, 13.07, 77.70)),
    ("Bangkok",        (13.65, 100.42, 13.81, 100.64)),
    ("Singapore",      (1.21, 103.60, 1.49, 104.04)),
    ("Kuala Lumpur",   (3.04, 101.60, 3.21, 101.76)),
    ("Jakarta",        (-6.38, 106.65, -6.08, 106.95)),
    ("Dubai",      (25.05, 55.13, 25.31, 55.43)),
    ("Doha",       (25.23, 51.42, 25.42, 51.64)),
    ("New York",   (40.66, -74.05, 40.85, -73.85)),
    ("Toronto",    (43.61, -79.48, 43.79, -79.25)),
    ("London",     (51.40, -0.35, 51.70, 0.13)),
]

def run_overpass(query, timeout=90):
    for mirror in MIRRORS:
        try:
            result = subprocess.run(
                ["curl", "-s", "-X", "POST", mirror,
                 "-H", "Content-Type: application/x-www-form-urlencoded",
                 "-d", f"data={query}",
                 "--max-time", str(timeout)],
                capture_output=True, timeout=timeout + 10
            )
            if result.returncode != 0 or len(result.stdout) < 30:
                continue
            if result.stdout[:1] == b"<":
                continue
            return json.loads(result.stdout), None
        except Exception:
            continue
    return None, "all mirrors failed"

results = []
print("=== OSM Website-Filtered Survey (29 cities) ===\n", flush=True)
print("Strategy: shop+website OR office+website (amenities excluded — only 6% web)\n", flush=True)

for name, (s, w, n, e) in CITIES:
    bbox = f"({s},{w},{n},{e})"
    # Lighter query: only shop and office with website/contact:website (2 unions, not 6)
    # Also get phone count in the same query
    query = f'[out:json][timeout:90];(nwr["shop"]["website"]{bbox};nwr["office"]["website"]{bbox};nwr["shop"]["contact:website"]{bbox};nwr["office"]["contact:website"]{bbox});out center;'
    
    t0 = time.time()
    data, err = run_overpass(query)
    elapsed = time.time() - t0
    
    if err or data is None:
        print(f"{name:<16} ERROR: {err}", flush=True)
        results.append({"name": name, "error": err, "total": 0})
    else:
        elements = data.get("elements", [])
        total = len(elements)
        with_phone = sum(1 for el in elements if el.get("tags", {}).get("phone") or el.get("tags", {}).get("contact:phone"))
        with_name = sum(1 for el in elements if el.get("tags", {}).get("name"))
        phone_pct = round(100*with_phone/total) if total else 0
        name_pct = round(100*with_name/total) if total else 0
        print(f"{name:<16} {total:>5} | phone {phone_pct:>2}% | name {name_pct:>2}% | {elapsed:.0f}s", flush=True)
        results.append({
            "name": name, "total": total,
            "phone_count": with_phone, "phone_pct": phone_pct,
            "name_pct": name_pct, "elapsed_s": round(elapsed, 1),
        })
    
    with open("/tmp/osm_website_filtered.json", "w") as f:
        json.dump(results, f, indent=2)
    
    time.sleep(3)

print("\n\n=== FINAL RESULTS ===\n")
print(f"{'City':<16} {'With Website':<14} {'Also Phone%':<12} {'Name%':<7}")
print("-" * 55)
grand_total = 0
grand_phone = 0
for r in results:
    if "error" in r and r.get("total", 0) == 0:
        print(f"{r['name']:<16} ERROR: {r.get('error','')[:30]}")
    else:
        print(f"{r['name']:<16} {r['total']:<14} {r['phone_pct']:<12} {r['name_pct']:<7}")
        grand_total += r["total"]
        grand_phone += r.get("phone_count", 0)

print("-" * 55)
print(f"{'TOTAL':<16} {grand_total:<14}")
if grand_total:
    print(f"\nTotal businesses with website across 29 cities: {grand_total}")
    print(f"Of those, also have phone: {grand_phone} ({round(100*grand_phone/grand_total)}%)")
print("\nDone. Results saved to /tmp/osm_website_filtered.json")
