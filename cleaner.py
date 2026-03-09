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
MIN_PRICE   = 100
MAX_PRICE   = 50_000
MIN_SURFACE = 10
MAX_SURFACE = 1_500
MAX_ROOMS   = 20

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


def clean_description(text: str | None) -> str | None:
    if not text:
        return None
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text if len(text) > 20 else None


def normalize_property_type(raw: str | None) -> str | None:
    if not raw:
        return None
    raw_lower = raw.lower()
    if any(w in raw_lower for w in ("appartement", "studio", "duplex", "flat")):
        return "apartment"
    if any(w in raw_lower for w in ("maison", "villa", "house")):
        return "house"
    return raw


def clean_document(doc: dict) -> dict:
    c = {}

    ad_id = doc.get("ad_id")
    c["source_id"]        = str(ad_id) if ad_id else None
    c["source"]           = "mubawab"
    c["country"]          = "TN"
    c["transaction_type"] = "rent"

    c["url"] = doc.get("url")

    c["property_type"] = normalize_property_type(doc.get("property_type"))

    c["city"] = doc.get("city") or None

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

    c["description"] = clean_description(doc.get("description"))
    c["title"]       = doc.get("title") or None

    images = doc.get("images")
    if isinstance(images, list):
        c["photos"]       = images
        c["photos_count"] = len(images)
    else:
        c["photos"]       = []
        c["photos_count"] = 0

    c["agency_name"] = doc.get("seller_name") or None

    feats = doc.get("features")
    if isinstance(feats, list) and feats:
        c["features"] = feats

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

def validate(doc: dict) -> tuple[bool, str | None]:
    price = doc.get("price")
    if not price or price < MIN_PRICE or price > MAX_PRICE:
        return False, "invalid_price"

    if not doc.get("source_id"):
        return False, "missing_source_id"

    if not doc.get("city"):
        return False, "missing_city"

    surface = doc.get("surface_m2")
    if surface and (surface < MIN_SURFACE or surface > MAX_SURFACE):
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
        "invalid_price": 0, "missing_source_id": 0, "missing_city": 0,
        "invalid_surface": 0, "aberrant_rooms": 0, "errors": 0,
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
        for k in ("invalid_price", "missing_source_id", "missing_city", "invalid_surface", "aberrant_rooms"):
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
