#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mubawab Data Cleaner — Locations (Incremental)
Normalise raw MongoDB → locations_clean collection.

Usage:
    python cleaner.py              # Incremental (only new docs)
    python cleaner.py --full       # Drop + recreate
    python cleaner.py --dry-run    # Preview, no writes
    python cleaner.py --sample 5   # Show N docs after cleaning
"""

import os
import re
import html
import unicodedata
import argparse
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne, ASCENDING
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

MONGODB_URI = os.getenv("MONGODB_URI", "")
if not MONGODB_URI:
    raise RuntimeError("MONGODB_URI is not set or empty")

MONGODB_DATABASE  = os.getenv("MONGO_MUBAWAB_DB", "mubawab")
SOURCE_COLLECTION = os.getenv("MONGO_MUBAWAB_COL", "locations")
CLEAN_COLLECTION  = os.getenv("MONGO_MUBAWAB_COL_CLEAN", "locations_clean")
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "500"))

# TND monthly rent thresholds
# Upper bounds removed — commercial properties (warehouses, factories) have
# legitimately large surfaces and high rents
MIN_PRICE   = 50
MIN_SURFACE = 5
MAX_ROOMS   = 50

# ============================================================
# CLEANING HELPERS
# ============================================================

def parse_price(raw) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str):
        m = re.search(r"[\d\s]+", raw.replace("\xa0", " ").replace(",", "."))
        if m:
            try:
                return float(m.group(0).replace(" ", ""))
            except ValueError:
                pass
    return None


def parse_surface(raw) -> float | None:
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def clean_description(raw: str | None) -> str | None:
    """Strip HTML tags and normalize whitespace. Preserves Arabic and other Unicode text."""
    if not raw:
        return None
    text = re.sub(r"<[^>]+>", " ", raw)
    text = html.unescape(text)
    # Normalize whitespace only — do NOT encode to ASCII (would destroy Arabic)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()
    return text if len(text) > 10 else None


def normalize_property_type(raw: str | None) -> str | None:
    if not raw:
        return None
    r = raw.lower()
    if any(w in r for w in ("appartement", "studio", "duplex", "flat", "شقة", "شقق")):
        return "apartment"
    if any(w in r for w in ("maison", "house", "منزل", "بيت", "بيوت")):
        return "house"
    if any(w in r for w in ("villa", "فيلا", "فيلات")):
        return "villa"
    if any(w in r for w in ("chambre", "room", "غرفة", "غرف")):
        return "room"
    if any(w in r for w in ("bureau", "office", "مكتب", "مكاتب")):
        return "office"
    if any(w in r for w in ("local commercial", "commerce", "retail", "محل", "محلات")):
        return "commercial"
    if any(w in r for w in ("ferme", "farm", "ضيعة", "ضيعات")):
        return "farm"
    if any(w in r for w in ("entrepôt", "warehouse", "مستودع")):
        return "warehouse"
    return raw


def clean_document(doc: dict) -> dict:
    c = {}

    ad_id = doc.get("ad_id")
    c["source_id"]        = str(ad_id) if ad_id else None
    c["source"]           = "mubawab"
    c["country"]          = "TN"
    c["transaction_type"] = "rent"

    # URLs (old scraper: "url"; new scraper: "url_fr" + "url_ar")
    c["url"]    = doc.get("url_fr") or doc.get("url")
    c["url_ar"] = doc.get("url_ar") or None

    # Property type — prefer FR, fallback to AR
    raw_type = doc.get("property_type_fr") or doc.get("property_type") or doc.get("property_type_ar")
    c["property_type"] = normalize_property_type(raw_type)

    # City — prefer FR, fallback to AR
    c["city"]    = doc.get("city_fr") or doc.get("city") or None
    c["city_ar"] = doc.get("city_ar") or None

    price = parse_price(doc.get("price"))
    c["price"]    = price
    c["currency"] = doc.get("currency", "TND")

    surface = parse_surface(doc.get("area_m2"))
    c["surface_m2"] = surface

    c["rooms"]     = doc.get("rooms")
    c["bedrooms"]  = doc.get("bedrooms")
    c["bathrooms"] = doc.get("bathrooms")

    lat = doc.get("latitude")
    lon = doc.get("longitude")
    if lat is not None and lon is not None:
        try:
            lat, lon = float(lat), float(lon)
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                c["latitude"]  = round(lat, 6)
                c["longitude"] = round(lon, 6)
        except (TypeError, ValueError):
            pass

    # Title — bilingual (new scraper) or single (old scraper)
    c["title"]    = doc.get("title_fr") or doc.get("title") or None
    c["title_ar"] = doc.get("title_ar") or None

    # Description — bilingual (new) or single (old)
    c["description"]    = clean_description(doc.get("description_fr") or doc.get("description"))
    c["description_ar"] = clean_description(doc.get("description_ar"))

    # Phone (new scraper)
    c["phone"] = doc.get("phone") or None

    images = doc.get("images")
    if isinstance(images, list):
        c["photos"]       = images
        c["photos_count"] = len(images)
    else:
        c["photos"]       = []
        c["photos_count"] = 0

    c["agency_name"] = doc.get("seller_name") or None

    # Features — bilingual
    feats_fr = doc.get("features_fr") or doc.get("features")
    feats_ar = doc.get("features_ar")
    if isinstance(feats_fr, list) and feats_fr:
        c["features"] = feats_fr
    if isinstance(feats_ar, list) and feats_ar:
        c["features_ar"] = feats_ar

    if price and surface and surface > 0:
        c["price_per_m2"] = round(price / surface, 2)
    if price and c.get("bedrooms") and c["bedrooms"] > 0:
        c["price_per_bedroom"] = round(price / c["bedrooms"], 2)

    c["scraped_at"] = doc.get("scraped_at")
    c["cleaned_at"] = datetime.now(timezone.utc)

    return {k: v for k, v in c.items() if v is not None and v != [] and v != ""}


# ============================================================
# VALIDATION
# ============================================================

EXCLUDED_TYPES = {"garage", "parking", "land"}


def validate(doc: dict) -> tuple[bool, str | None]:
    if doc.get("property_type") in EXCLUDED_TYPES:
        return False, "excluded_type"

    price = doc.get("price")
    if not price or price < MIN_PRICE:
        return False, "invalid_price"

    if not doc.get("source_id"):
        return False, "missing_source_id"

    surface = doc.get("surface_m2")
    if surface and surface < MIN_SURFACE:
        return False, "invalid_surface"

    rooms = doc.get("bedrooms")
    if rooms and rooms > MAX_ROOMS:
        return False, "aberrant_rooms"

    return True, None


# ============================================================
# DB HELPERS
# ============================================================

def connect_db():
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DATABASE]
    return client, db


def ensure_indexes(col):
    col.create_index([("source_id", ASCENDING)], unique=True, name="source_id_unique")
    col.create_index([("city", ASCENDING)])
    col.create_index([("price", ASCENDING)])
    col.create_index([("surface_m2", ASCENDING)])
    col.create_index([("property_type", ASCENDING)])
    col.create_index([("country", ASCENDING)])
    return col


def insert_batch(col, batch: list) -> tuple[int, int]:
    if not batch:
        return 0, 0
    ops = [
        UpdateOne(
            {"source_id": doc["source_id"]},
            {"$set": doc},
            upsert=True,
        )
        for doc in batch if doc.get("source_id")
    ]
    if not ops:
        return 0, 0
    r = col.bulk_write(ops, ordered=False)
    return r.upserted_count, r.modified_count


# ============================================================
# PIPELINE
# ============================================================

def run(source_col, clean_col, dry_run=False):
    total = source_col.count_documents({})
    print(f"   Source total: {total} docs")

    if not dry_run and clean_col is not None:
        existing_ids = {
            d.get("source_id") for d in clean_col.find({}, {"source_id": 1, "_id": 0})
            if d.get("source_id")
        }
        print(f"   Already cleaned: {len(existing_ids)}")
    else:
        existing_ids = set()

    if existing_ids:
        query = {"ad_id": {"$nin": list(existing_ids)}}
    else:
        query = {}

    pending = source_col.count_documents(query)
    print(f"   Pending: {pending}\n")

    if pending == 0:
        print("   Nothing new to clean.")
        return

    stats = {
        "cleaned": 0, "inserted": 0, "updated": 0,
        "invalid_price": 0, "missing_source_id": 0,
        "invalid_surface": 0, "aberrant_rooms": 0, "excluded_type": 0, "errors": 0,
    }

    batch = []
    cursor = source_col.find(query, batch_size=BATCH_SIZE, no_cursor_timeout=True)
    try:
        for i, doc in enumerate(cursor):
            try:
                cleaned = clean_document(doc)
                stats["cleaned"] += 1

                valid, reason = validate(cleaned)
                if not valid:
                    stats[reason] = stats.get(reason, 0) + 1
                    continue

                cleaned.pop("_id", None)

                if dry_run:
                    stats["inserted"] += 1
                    continue

                batch.append(cleaned)
                if len(batch) >= BATCH_SIZE:
                    ins, upd = insert_batch(clean_col, batch)
                    stats["inserted"] += ins
                    stats["updated"]  += upd
                    batch = []
                    print(f"   {i+1}/{pending} cleaned …", end="\r", flush=True)

            except Exception as e:
                stats["errors"] += 1
                if stats["errors"] <= 5:
                    print(f"\n   Error on {doc.get('ad_id')}: {str(e)[:100]}")

        if batch and not dry_run:
            ins, upd = insert_batch(clean_col, batch)
            stats["inserted"] += ins
            stats["updated"]  += upd
    finally:
        cursor.close()

    print_stats(stats, dry_run)


def print_stats(s, dry_run=False):
    print(f"\n{'='*60}")
    print(f"CLEANING RESULTS {'(DRY RUN)' if dry_run else ''}")
    print(f"{'='*60}")
    print(f"   Processed:  {s['cleaned']}")
    print(f"   Inserted:   {s['inserted']}")
    print(f"   Updated:    {s['updated']}")
    rejected = s["cleaned"] - s["inserted"] - s["updated"]
    if rejected > 0:
        print(f"   Rejected:   {rejected}")
        for k in ("invalid_price", "missing_source_id", "invalid_surface", "aberrant_rooms", "excluded_type"):
            if s.get(k):
                print(f"      {k}: {s[k]}")
    if s["errors"]:
        print(f"   Errors:     {s['errors']}")
    print(f"{'='*60}")


def show_sample(col, n=3):
    print(f"\nSAMPLE DOCS ({n}):")
    for doc in col.find({}, {"_id": 0}).limit(n):
        print("─" * 60)
        for k, v in doc.items():
            if k == "photos":
                print(f"   {k}: [{len(v)} urls]")
            elif k == "description":
                print(f"   {k}: {str(v)[:80]}...")
            else:
                print(f"   {k}: {v}")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Mubawab Cleaner")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--full",    action="store_true")
    parser.add_argument("--sample",  type=int, default=0)
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("MUBAWAB CLEANER — LOCATIONS")
    print(f"   {SOURCE_COLLECTION} → {CLEAN_COLLECTION}")
    mode = "DRY RUN" if args.dry_run else ("FULL RE-CLEAN" if args.full else "INCREMENTAL")
    print(f"   Mode: {mode}")
    print("=" * 60 + "\n")

    client, db = connect_db()
    source_col = db[SOURCE_COLLECTION]

    if args.dry_run:
        run(source_col, None, dry_run=True)
    elif args.full:
        clean_col = db[CLEAN_COLLECTION]
        clean_col.drop()
        print(f"   '{CLEAN_COLLECTION}' reset (full mode)")
        clean_col = ensure_indexes(db[CLEAN_COLLECTION])
        run(source_col, clean_col)
    else:
        clean_col = ensure_indexes(db[CLEAN_COLLECTION])
        run(source_col, clean_col)
        if args.sample > 0:
            show_sample(clean_col, args.sample)
        print(f"\n   Done! '{CLEAN_COLLECTION}': {clean_col.count_documents({})} total docs")

    client.close()


if __name__ == "__main__":
    main()
