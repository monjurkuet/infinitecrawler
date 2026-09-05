"""daemons/query_keywords.py — data-only literal arrays for query generation.

Extracted from daemons/query_generator.py in 2026-08-12 (B3) so the ~120 LOC
of literal city / market / keyword arrays can be edited without touching the
rotation logic. All values are pure data with no functions.
"""

# 15 Bangladeshi cities: (english, bangla, lat, lng) — coords anchor the GMaps
# search region. Annotated per the original file so edits stay self-documenting.
BD_CITIES = [
    ("Chattogram",  "চট্টগ্রাম",   22.3569,  91.7832),
    ("Sylhet",      "সিলেট",       24.8949,  91.8687),
    ("Khulna",      "খুলনা",       22.8456,  89.5403),
    ("Rajshahi",    "রাজশাহী",     24.3636,  88.6241),
    ("Barishal",    "বরিশাল",      22.7010,  90.3535),
    ("Rangpur",     "রংপুর",       25.7439,  89.2752),
    ("Mymensingh",  "ময়মনসিংহ",  24.7471,  90.4203),
    ("Cumilla",     "কুমিল্লা",    23.4607,  91.1809),
    ("Bogura",      "বগুড়া",      24.8484,  89.3733),
    ("Jashore",     "যশোর",        23.1684,  89.2123),
    ("Cox's Bazar", "কক্সবাজার",   21.4272,  92.0058),
    ("Narayanganj", "নারায়ণগঞ্জ", 23.6238,  90.5000),
    ("Gazipur",     "গাজীপুর",     23.9919,  90.4203),
    ("Feni",        "ফেনী",        23.0149,  91.3953),
    ("Narsingdi",   "নরসিংদী",    23.9889,  90.4650),
]


# National / international coordinates (lat, lng, zoom) per market.
# Used to build region-anchored searches; format: KEYWORD|lat|lng|zoom
# Fairbury, Nebraska region (user-requested niche market 2026-09-06)
# Niche: handyman + REO (real-estate-owned) property preservation crews.
# Coverage: Jefferson County + surrounding trade area — Fairbury itself is
# tiny (pop ~4k); preservation crews for the county are sourced from the
# anchor towns below (county seats, regional hubs, metro wholesalers).
FAIRBURY_ANCHORS = [
    # Jefferson County towns (the actual target territory)
    ("Fairbury", 40.1372, -97.1806),   # county seat
    ("Endicott", 40.0817, -97.0962),
    ("Diller", 40.1094, -96.9398),
    ("Plymouth", 40.3025, -96.9894),
    ("Jansen", 40.1806, -97.0831),
    ("Steele City", 40.0375, -97.0228),
    # Neighboring county seats & trade hubs (crews come FROM here)
    ("Beatrice", 40.2683, -96.7467),   # Gage County seat
    ("Wilber", 40.4811, -96.9606),     # Saline County seat
    ("Hebron", 40.1686, -97.5857),     # Thayer County seat
    ("Geneva", 40.5267, -97.5961),     # Fillmore County seat
    ("Crete", 40.6278, -96.9614),
    ("York", 40.8670, -97.5920),
    ("Hastings", 40.5863, -98.3899),   # regional hub
    ("Lincoln", 40.8136, -96.7026),    # state capital / metro
    ("Omaha", 41.2565, -95.9345),      # metro wholesalers
]
# Kept for back-compat: primary anchor.
FAIRBURY_COORD = (40.1372, -97.1806)

# Handyman + REO preservation keywords (county-scale maps niche, not CITY niche).
FAIRBURY_NICHE_KEYWORDS = [
    "handyman",
    "handyman services",
    "property preservation",
    "REO property preservation",
    "REO preservation contractor",
    "foreclosure cleanup",
    "property maintenance company",
    "home repair services",
    "general contractor",
    "remodeling contractor",
    "property preservation services",
    "field services contractor",
]

BD_COORD = (23.685, 90.3563, 7)
INTERNATIONAL_MARKETS = [
    ("USA", 37.0902, -95.7129, 5),
    ("UK", 55.3781, -3.4360, 6),
    ("Australia", -25.2744, 133.7751, 4),
    ("Canada", 56.1304, -106.3468, 4),
    ("UAE", 23.4241, 53.8478, 6),
    ("Saudi Arabia", 23.8859, 45.0792, 6),
    ("Germany", 51.1657, 10.4515, 6),
    ("France", 46.6034, 1.8883, 6),
    ("Italy", 41.8719, 12.5674, 6),
    ("Netherlands", 52.1326, 5.2913, 7),
    ("Belgium", 50.8503, 4.3517, 7),
    ("Sweden", 60.1282, 18.6435, 5),
    ("Switzerland", 46.8182, 8.2275, 7),
    ("Austria", 47.5162, 14.5501, 7),
    ("Denmark", 56.2639, 9.5018, 6),
    ("Norway", 60.4720, 8.4689, 5),
    ("Singapore", 1.3521, 103.8198, 10),
    ("Malaysia", 4.2105, 101.9758, 6),
    ("Japan", 36.2048, 138.2529, 5),
    ("South Korea", 35.9078, 127.7669, 6),
    ("Hong Kong", 22.3193, 114.1694, 10),
    ("Qatar", 25.3548, 51.1839, 8),
    ("Oman", 21.4735, 55.9754, 6),
    ("Kuwait", 29.3117, 47.4818, 8),
    ("Bahrain", 25.9304, 50.6378, 9),
    ("India", 20.5937, 78.9629, 5),
    ("South Africa", -30.5595, 22.9375, 5),
    ("Brazil", -14.2350, -51.9253, 4),
]


# Built-in keyword fallback used when the BPT sectors.yaml is missing (the
# file lives in a sibling repo, `business-plan-template`, which is not always
# present). Keeps the daemon productive instead of crash-looping on empty
# pools. These mirror the buyer business types from the sector configs.
DEFAULT_KEYWORDS_EN = [
    "manufacturing company", "factory", "warehouse", "logistics company",
    "transport company", "trucking company", "freight forwarder",
    "buying house", "apparel sourcing", "garments factory", "textile mill",
    "tailor shop", "cloth store", "departmental store", "supermarket",
    "grocery store", "wholesale market", "pharmacy", "medical store",
    "hospital", "clinic", "diagnostic center", "dental clinic",
    "eye hospital", "nursing home", "hotel", "restaurant", "cafe",
    "coffee shop", "fast food", "bakery", "sweet shop", "catering service",
    "guest house", "resort", "motel", "rest house", "travel agency",
    "airline agency", "tour operator", "bus service", "car rental",
    "tire shop", "auto workshop", "car servicing", "motorcycle showroom",
    "bike service center", "electronics shop", "mobile phone shop",
    "computer shop", "computer training center", "online service",
    "it company", "software company", "web design agency",
    "digital marketing agency", "seo agency", "printing press",
    "stationery shop", "book shop", "school", "college", "training center",
    "coaching center", "bank", "atm", "insurance company", "finance company",
    "remittance service", "microfinance", "nbfi", "stock broker",
    "real estate developer", "construction company", "interior design",
    "architect", "engineering consultant", "event venue", "convention hall",
    "community center", "gym", "salon", "spa", "jewelry shop",
    "furniture shop", "hardware store", "paint shop", "cement supplier",
    "steel supplier", "agro farm", "poultry farm", "fish farm", "dairy farm",
    "feed mill", "seed store", "fertilizer shop", "cold storage",
    "food processing", "rice mill", "flour mill", "oil mill", "ice factory",
    "beverage distributor", "cosmetics shop", "perfume shop", "toy shop",
    "sports shop", "gift shop", "pet shop", "laundry", "dry cleaner",
    "security service", "cleaning service", "pest control",
    "cctv installation", "electrical shop", "ac service center",
    "plumbing service", "packaging company", "ceramics factory",
    "brick factory", "plastic factory", "pharmaceutical company",
    "exporter", "importer", "supplier", "distributor", "dealer",
    "showroom", "agency", "office", "head office", "branch office",
]
DEFAULT_KEYWORDS_BN = [
    "উৎপাদন কারখানা", "ফ্যাক্টরি", "গুদাম", "পরিবহন কোম্পানি",
    "ট্রাকিং কোম্পানি", "ফ্রেইট ফরওয়ার্ডার", "বায়িং হাউস",
    "গার্মেন্টস ফ্যাক্টরি", "টেক্সটাইল মিল", "দর্জি দোকান",
    "কাপড়ের দোকান", "ডিপার্টমেন্টাল স্টোর", "সুপারমার্কেট",
    "মুদি দোকান", "পাইকারি বাজার", "ফার্মেসি", "মেডিকেল স্টোর",
    "হাসপাতাল", "ক্লিনিক", "ডায়াগনস্টিক সেন্টার", "ডেন্টাল ক্লিনিক",
    "চক্ষু হাসপাতাল", "নার্সিং হোম", "হোটেল", "রেস্টুরেন্ট", "ক্যাফে",
    "কফি শপ", "ফাস্ট ফুড", "বেকারি", "মিষ্টির দোকান", "ক্যাটারিং সার্ভিস",
    "গেস্ট হাউস", "রিসোর্ট", "মোটেল", "রেস্ট হাউস", "ট্রাভেল এজেন্সি",
    "এয়ারলাইন এজেন্সি", "ট্যুর অপারেটর", "বাস সার্ভিস", "কার রেন্টাল",
    "টায়ার শপ", "অটো ওয়ার্কশপ", "কার সার্ভিসিং", "মোটরসাইকেল শোরুম",
    "বাইক সার্ভিস সেন্টার", "ইলেকট্রনিক্স শপ", "মোবাইল ফোন শপ",
    "কম্পিউটার শপ", "কম্পিউটার প্রশিক্ষণ কেন্দ্র", "অনলাইন সার্ভিস",
    "আইটি কোম্পানি", "সফটওয়্যার কোম্পানি", "ওয়েব ডিজাইন এজেন্সি",
    "ডিজিটাল মার্কেটিং এজেন্সি", "সিও এজেন্সি", "প্রিন্টিং প্রেস",
    "স্টেশনারি দোকান", "বইয়ের দোকান", "স্কুল", "কলেজ",
    "প্রশিক্ষণ কেন্দ্র", "কোচিং সেন্টার", "ব্যাংক", "এটিএম",
    "ইনস্যুরেন্স কোম্পানি", "ফাইন্যান্স কোম্পানি", "রেমিট্যান্স সার্ভিস",
    "মাইক্রোফাইন্যান্স", "এনবিএফআই", "স্টক ব্রোকার",
    "রিয়েল এস্টেট ডেভেলপার", "কনস্ট্রাকশন কোম্পানি", "ইন্টেরিয়র ডিজাইন",
    "স্থপতি", "ইঞ্জিনিয়ারিং কনসালট্যান্ট", "ইভেন্ট ভেন্যু",
    "কনভেনশন হল", "কমিউনিটি সেন্টার", "জিম", "সেলুন", "স্পা",
    "জুয়েলারি শপ", "ফার্নিচার শপ", "হার্ডওয়্যার দোকান", "পেইন্ট শপ",
    "সিমেন্ট সাপ্লায়ার", "স্টিল সাপ্লায়ার", "এগ্রো ফার্ম",
    "পোল্ট্রি ফার্ম", "মাছের খামার", "ডেইরি ফার্ম", "ফিড মিল",
    "বীজের দোকান", "সার দোকান", "কোল্ড স্টোরেজ",
    "ফুড প্রসেসিং", "রাইস মিল", "ফ্লাওয়ার মিল", "অয়েল মিল",
    "আইস ফ্যাক্টরি", "বেভারেজ ডিস্ট্রিবিউটর", "কসমেটিক্স শপ",
    "পারফিউম শপ", "খেলনার দোকান", "স্পোর্টস শপ", "গিফট শপ",
    "পোষা প্রাণীর দোকান", "লন্ড্রি", "ড্রাই ক্লিনার", "সিকিউরিটি সার্ভিস",
    "ক্লিনিং সার্ভিস", "পেস্ট কন্ট্রোল", "সিসিটিভি ইনস্টলেশন",
    "ইলেকট্রিক্যাল শপ", "এসি সার্ভিস সেন্টার", "প্লাম্বিং সার্ভিস",
    "প্যাকেজিং কোম্পানি", "সিরামিকস ফ্যাক্টরি", "ইটের কারখানা",
    "প্লাস্টিক ফ্যাক্টরি", "ফার্মাসিউটিক্যাল কোম্পানি", "রপ্তানিকারক",
    "আমদানিকারক", "সাপ্লায়ার", "ডিস্ট্রিবিউটর", "ডিলার", "শোরুম",
    "এজেন্সি", "অফিস", "প্রধান কার্যালয়", "শাখা অফিস",
]


FALLBACK_SECTOR_CONFIG = {
    "fallback": {
        "status": "active",
        "target_business_types": {
            "en": DEFAULT_KEYWORDS_EN,
            "bn": DEFAULT_KEYWORDS_BN,
        },
    },
}
