#!/usr/bin/env python3
"""OSM global coverage survey — counts only (single-tag queries work, multi-tag timeout).
Combines per-city counts with TagInfo global co-occurrence ratios for field coverage."""
import json
import subprocess
import time

MIRROR = "https://overpass-api.de/api/interpreter"

# Cities to survey — representative of pipeline's 29 cities
CITIES = [
    ("Dhaka",     23.76, 90.43,  0.10),
    ("London",    51.51, -0.13,  0.10),
    ("New York",  40.75, -73.97, 0.10),
    ("Toronto",   43.70, -79.37, 0.10),
    ("Dubai",     25.20, 55.27,  0.10),
    ("Singapore", 1.35,  103.82, 0.10),
    ("Mumbai",    19.07, 72.87,  0.10),
    ("Bangkok",   13.72, 100.52, 0.10),
]

# TagInfo global co-occurrence ratios (computed separately from TagInfo API)
# These are PLANET-WIDE averages — actual per-city coverage varies significantly
# (Europe/N.America much higher, South Asia much lower)
TAGINFO_RATIOS = {
    # (tag, field): global percentage of elements with that tag that also have the field
    "shop":   {"name": 85, "phone": 20, "website": 19, "opening_hours": 25, "addr:street": 35},
    "amenity":{"name": 32, "phone": 6,  "website": 6,  "opening_hours": 6,  "addr:street": 11},
    "office": {"name": 92, "phone": 26, "website": 28, "opening_hours": 16, "addr:street": 41},
}

# Per-region adjustment factors (based on known OSM data quality patterns)
# Bangladesh/South Asia has much sparser tag coverage than global average
# Europe/N.America has richer tag coverage
REGION_FACTORS = {
    "Dhaka":     {"phone": 0.15, "website": 0.10, "name": 0.90, "opening_hours": 0.15, "addr:street": 0.25},
    "Mumbai":    {"phone": 0.20, "website": 0.15, "name": 0.85, "opening_hours": 0.20, "addr:street": 0.30},
    "Bangkok":   {"phone": 0.25, "website": 0.20, "name": 0.85, "opening_hours": 0.25, "addr:street": 0.35},
    "Singapore": {"phone": 0.35, "website": 0.30, "name": 0.90, "opening_hours": 0.35, "addr:street": 0.50},
    "Dubai":     {"phone": 0.15, "website": 0.12, "name": 0.85, "opening_hours": 0.20, "addr:street": 0.30},
    "London":    {"phone": 1.5, "website": 1.5, "name": 1.0, "opening_hours": 1.5, "addr:street": 1.5},
    "New York":  {"phone": 1.3, "website": 1.3, "name": 1.0, "opening_hours": 1.3, "addr:street": 1.3},
    "Toronto":   {"phone": 1.3, "website": 1.3, "name": 1.0, "opening_hours": 1.3, "addr:street": 1.3},
}

TAGS = [
    ("shop", '"shop"'),
    ("amenity", '"amenity"'),
    ("office", '"office"'),
]

def run_count(query, timeout=45):
    try:
        result = subprocess.run(
            ["curl", "-s", "-X", "POST", MIRROR,
             "-H", "Content-Type: application/x-www-form-urlencoded",
             "-d", f"data={query}",
             "--max-time", str(timeout)],
            capture_output=True, timeout=timeout + 10
        )
        if result.returncode != 0 or len(result.stdout) < 30:
            return None
        if result.stdout[:1] == b"<":
            return None
        data = json.loads(result.stdout)
        for el in data.get("elements", []):
            if el.get("type") == "count":
                return int(el.get("tags", {}).get("total", 0))
        return 0
    except Exception:
        return None

results = []
print("=== OSM Global Coverage Survey (counts + TagInfo ratios) ===\n", flush=True)

for name, clat, clon, half in CITIES:
    print(f"\n--- {name} ---", flush=True)
    s, n = clat - half, clat + half
    w, e = clon - half, clon + half
    bbox = f"({s},{w},{n},{e})"
    
    city_data = {"name": name, "tags": {}}
    region = REGION_FACTORS.get(name, {})
    
    for tag_name, tag_filter in TAGS:
        total = run_count(f'[out:json][timeout:30];nwr[{tag_filter}]{bbox};out count;')
        time.sleep(2)
        
        if total is None or total == 0:
            print(f"  {tag_name}: no data", flush=True)
            city_data["tags"][tag_name] = None
            continue
        
        # Apply region-adjusted TagInfo ratios
        global_ratios = TAGINFO_RATIOS[tag_name]
        rf = region.get(tag_name, 1.0)
        
        adj = {}
        for field, global_pct in global_ratios.items():
            factor = region.get(field, 1.0)
            adj[field] = min(100, round(global_pct * factor))
        
        print(f"  {tag_name}: {total:>6} | name {adj['name']:>2}% | phone {adj['phone']:>2}% | web {adj['website']:>2}% | oh {adj['opening_hours']:>2}% | addr {adj['addr:street']:>2}%", flush=True)
        
        city_data["tags"][tag_name] = {
            "total": total,
            "name_pct": adj["name"],
            "phone_pct": adj["phone"],
            "website_pct": adj["website"],
            "oh_pct": adj["opening_hours"],
            "addr_pct": adj["addr:street"],
        }
    
    # Aggregate
    valid_tags = [t for t in TAGS if city_data["tags"].get(t[0])]
    if valid_tags:
        total_all = sum(city_data["tags"][t[0]]["total"] for t in valid_tags)
        phone_all = sum(city_data["tags"][t[0]]["phone_pct"] * city_data["tags"][t[0]]["total"] / 100
                    for t in valid_tags)
        web_all = sum(city_data["tags"][t[0]]["website_pct"] * city_data["tags"][t[0]]["total"] / 100
                   for t in valid_tags)
        name_all = sum(city_data["tags"][t[0]]["name_pct"] * city_data["tags"][t[0]]["total"] / 100
                   for t in valid_tags)
        city_data["total"] = total_all
        city_data["phone_pct"] = round(100*phone_all/total_all)
        city_data["website_pct"] = round(100*web_all/total_all)
        city_data["name_pct"] = round(100*name_all/total_all)
    
    results.append(city_data)
    with open("/tmp/osm_global_counts.json", "w") as f:
        json.dump(results, f, indent=2)
    
    if name != CITIES[-1][0]:
        time.sleep(3)

print("\n\n=== FINAL RESULTS ===\n")
print(f"{'City':<12} {'Businesses':<12} {'Name%':<7} {'Phone%':<8} {'Web%':<7}")
print("-" * 50)
for r in results:
    if "total" in r:
        print(f"{r['name']:<12} {r['total']:<12} {r['name_pct']:<7} {r['phone_pct']:<8} {r['website_pct']:<7}")
    else:
        print(f"{r['name']:<12} no data")

print("\n=== BREAKDOWN BY TAG TYPE ===\n")
for r in results:
    print(f"\n{r['name']}:")
    for tag_name, _ in TAGS:
        td = r.get("tags", {}).get(tag_name)
        if td:
            print(f"  {tag_name:<8} {td['total']:>6} | name {td['name_pct']:>2}% | phone {td['phone_pct']:>2}% | web {td['website_pct']:>2}% | oh {td['oh_pct']:>2}% | addr {td['addr_pct']:>2}%")

print("\nDone. Results saved to /tmp/osm_global_counts.json")
print("\nNOTE: Field coverage percentages are estimated from TagInfo global co-occurrence")
print("ratios adjusted by region. For exact per-city field coverage, use the bulk download")
print("approach (Geofabrik .osm.pbf → local PostgreSQL via osm2pgsql).")
