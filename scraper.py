"""
Mubawab.tn Scraper — Locations FR + AR, tous types sauf terrains
"""

import os
import re
import json
import time
import logging
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from storage import upload_images, check_b2

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("mubawab")

MONGO_URI        = os.environ["MONGODB_URI"]
DB_NAME          = os.getenv("MONGO_MUBAWAB_DB",  "mubawab")
COLLECTION_NAME  = os.getenv("MONGO_MUBAWAB_COL", "locations")

BASE_URL = "https://www.mubawab.tn"

# ─── Property types (no terrains/land) ────────────────────────────
# (name, fr_search_url, ar_search_url)
ZONES = [
    ("Appartements",
     "https://www.mubawab.tn/fr/sc/appartements-a-louer",
     "https://www.mubawab.tn/ar/sc/%D8%B4%D9%82%D9%82-%D9%84%D9%84%D8%A5%D9%8A%D8%AC%D8%A7%D8%B1"),
    ("Maisons",
     "https://www.mubawab.tn/fr/sc/maisons-a-louer",
     "https://www.mubawab.tn/ar/sc/%D9%85%D9%86%D8%A7%D8%B2%D9%84-%D9%84%D9%84%D8%A5%D9%8A%D8%AC%D8%A7%D8%B1"),
    ("Villas",
     "https://www.mubawab.tn/fr/sc/villas-a-louer",
     "https://www.mubawab.tn/ar/sc/%D9%81%D9%8A%D9%84%D8%A7%D8%AA-%D9%84%D9%84%D8%A5%D9%8A%D8%AC%D8%A7%D8%B1"),
    ("Chambres",
     "https://www.mubawab.tn/fr/sc/chambres-a-louer",
     "https://www.mubawab.tn/ar/sc/%D8%BA%D8%B1%D9%81-%D9%84%D9%84%D8%A5%D9%8A%D8%AC%D8%A7%D8%B1"),
    ("Locaux commerciaux",
     "https://www.mubawab.tn/fr/sc/locaux-commerciaux-a-louer",
     "https://www.mubawab.tn/ar/sc/%D9%85%D8%AD%D9%84%D8%A7%D8%AA-%D8%AA%D8%AC%D8%A7%D8%B1%D9%8A%D8%A9-%D9%84%D9%84%D8%A5%D9%8A%D8%AC%D8%A7%D8%B1"),
    ("Bureaux",
     "https://www.mubawab.tn/fr/sc/bureaux-a-louer",
     "https://www.mubawab.tn/ar/sc/%D9%85%D9%83%D8%A7%D8%AA%D8%A8-%D9%84%D9%84%D8%A5%D9%8A%D8%AC%D8%A7%D8%B1"),
    ("Fermes",
     "https://www.mubawab.tn/fr/sc/fermes-a-louer",
     "https://www.mubawab.tn/ar/sc/%D8%B6%D9%8A%D8%B9%D8%A7%D8%AA-%D9%84%D9%84%D8%A5%D9%8A%D8%AC%D8%A7%D8%B1"),
    ("Immobilier divers",
     "https://www.mubawab.tn/fr/sc/immobilier-divers-a-louer",
     "https://www.mubawab.tn/ar/sc/%D8%B9%D9%82%D8%A7%D8%B1%D8%A7%D8%AA-%D9%85%D8%AA%D9%86%D9%88%D8%B9%D8%A9-%D9%84%D9%84%D8%A5%D9%8A%D8%AC%D8%A7%D8%B1"),
]

HEADERS_FR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
HEADERS_AR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ar-TN,ar;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DELAY     = int(os.getenv("DELAY_BETWEEN_REQUESTS", "2"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "300"))


# ══════════════════════════════════════════════════════════════════
# MONGODB
# ══════════════════════════════════════════════════════════════════

def get_collection():
    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]
    col.create_index("ad_id", unique=True)
    return col


# ══════════════════════════════════════════════════════════════════
# LINK DISCOVERY
# ══════════════════════════════════════════════════════════════════

def extract_id_from_url(url):
    m = re.search(r"/(?:pa|a)/(\d+)/", url)
    return m.group(1) if m else None


def get_page_url(base_url, page_num):
    return base_url if page_num <= 1 else f"{base_url}:p:{page_num}"


def get_ids_from_page(session, page_url, lang="fr"):
    """Return set of ad_ids and dicts {id: fr_url}, {id: ar_url} found on a search result page."""
    headers = HEADERS_FR if lang == "fr" else HEADERS_AR
    resp = session.get(page_url, headers=headers, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    ids, fr_urls, ar_urls = set(), {}, {}

    def register(href):
        m = re.search(r"/(fr|ar)/(pa|a)/(\d+)/", href)
        if not m:
            return
        lang_found, _, ad_id = m.group(1), m.group(2), m.group(3)
        ids.add(ad_id)
        full = urljoin(BASE_URL, href)
        if lang_found == "fr":
            fr_urls[ad_id] = full
        else:
            ar_urls[ad_id] = full

    for box in soup.select(".listingBox[linkRef]"):
        register(box.get("linkRef", ""))

    if not ids:
        for a in soup.select("a[href]"):
            register(a.get("href", ""))

    return ids, fr_urls, ar_urls


# ══════════════════════════════════════════════════════════════════
# DETAIL PAGE PARSING
# ══════════════════════════════════════════════════════════════════

def extract_json_ld(soup):
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            d = json.loads(script.string)
            if d.get("@type") == "RealEstateListing":
                return d
        except (json.JSONDecodeError, TypeError):
            continue
    return None


def parse_detail_page(soup):
    """Parse one detail page (FR or AR). Returns a dict."""
    d = {}

    ld = extract_json_ld(soup)
    if ld:
        d["title"]       = ld.get("name", "")
        d["description"] = ld.get("description", "")

        offers = ld.get("offers", {})
        d["price"]    = offers.get("price")
        d["currency"] = offers.get("priceCurrency", "TND")

        item = ld.get("itemOffered", {})
        d["rooms"]     = item.get("numberOfRooms")
        d["bedrooms"]  = item.get("numberOfBedrooms")
        d["bathrooms"] = item.get("numberOfBathroomsTotal")
        fs = item.get("floorSize", {})
        d["area_m2"]   = fs.get("value")
        addr = item.get("address", {})
        d["city"]    = addr.get("addressLocality", "")
        d["country"] = addr.get("addressCountry", "")

        seller = ld.get("seller", {})
        d["seller_name"] = seller.get("name", "")
        d["seller_type"] = seller.get("@type", "")
        d["phone_ld"]    = seller.get("telephone")  # might be present

        imgs = ld.get("image", [])
        d["raw_images"] = imgs if isinstance(imgs, list) else [imgs]

    else:
        # HTML fallback
        h1 = soup.find("h1", class_="searchTitle")
        d["title"] = h1.get_text(strip=True) if h1 else ""

        price_tag = soup.find("h3", class_="orangeTit")
        if price_tag:
            txt = price_tag.get_text(strip=True).replace("\xa0", " ")
            m = re.search(r"([\d\s]+)", txt)
            if m:
                d["price"] = float(m.group(1).replace(" ", ""))
        d["currency"] = "TND"

        for span in soup.select(".adDetailFeature span"):
            txt = span.get_text(strip=True)
            if "m²" in txt:
                m = re.search(r"(\d+)", txt)
                if m: d["area_m2"] = int(m.group(1))
            elif re.search(r"Pièce|غرفة", txt):
                m = re.search(r"(\d+)", txt)
                if m: d["rooms"] = int(m.group(1))
            elif re.search(r"Chambre|غرفة نوم", txt):
                m = re.search(r"(\d+)", txt)
                if m: d["bedrooms"] = int(m.group(1))
            elif re.search(r"bain|حمام", txt, re.I):
                m = re.search(r"(\d+)", txt)
                if m: d["bathrooms"] = int(m.group(1))

        desc = soup.find("div", class_="blockProp")
        if desc:
            p = desc.find("p")
            d["description"] = p.get_text(separator="\n", strip=True) if p else ""

        d["raw_images"] = [
            img.get("src", "")
            for img in soup.select(".picturesGallery img")
            if "mubawab-media" in img.get("src", "")
        ]

    # Location text
    loc = soup.find("h3", class_="greyTit")
    if loc:
        d["location_text"] = loc.get_text(strip=True)

    # Coordinates
    lat = soup.find("input", {"id": "latField"})
    lng = soup.find("input", {"id": "lngField"})
    if lat and lng:
        try:
            d["latitude"]  = float(lat["value"])
            d["longitude"] = float(lng["value"])
        except (ValueError, KeyError):
            pass

    # Property type from main features
    for blk in soup.select(".adMainFeature"):
        lbl = blk.select_one(".adMainFeatureContentLabel")
        val = blk.select_one(".adMainFeatureContentValue")
        if lbl and val and re.search(r"Type de bien|نوع العقار", lbl.get_text()):
            d["property_type"] = val.get_text(strip=True)
            break

    # Feature lists
    d["features"] = [
        f.get_text(strip=True) for f in soup.select(".adFeature .fSize11")
        if f.get_text(strip=True)
    ]
    d["main_features"] = {}
    for blk in soup.select(".adMainFeature"):
        lbl = blk.select_one(".adMainFeatureContentLabel")
        val = blk.select_one(".adMainFeatureContentValue")
        if lbl and val:
            d["main_features"][lbl.get_text(strip=True).rstrip(":")] = val.get_text(strip=True)

    return d


# ══════════════════════════════════════════════════════════════════
# PHONE EXTRACTION
# ══════════════════════════════════════════════════════════════════

def get_phone(session, ad_id, referer_url):
    """Click 'Appeler' equivalent — POST to Mubawab's phone AJAX endpoint."""
    try:
        resp = session.post(
            "https://www.mubawab.tn/fr/ajax/getphone.html",
            data={"adId": ad_id},
            headers={
                "X-Requested-With": "XMLHttpRequest",
                "Referer": referer_url,
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://www.mubawab.tn",
            },
            timeout=10,
        )
        if resp.ok:
            try:
                data = resp.json()
                phone = (data.get("phone") or data.get("telephone")
                         or data.get("phoneNumber") or data.get("sellerPhone"))
                if phone:
                    return str(phone).strip()
            except json.JSONDecodeError:
                text = resp.text.strip()
                m = re.search(r"\+?[\d\s\-\.]{8,}", text)
                if m:
                    return m.group(0).strip()
    except Exception as e:
        log.debug(f"[ad {ad_id}] Phone AJAX failed: {e}")
    return None


# ══════════════════════════════════════════════════════════════════
# COMBINED DETAIL SCRAPE (FR + AR)
# ══════════════════════════════════════════════════════════════════

def scrape_listing(session, ad_id, fr_url, ar_url):
    """Fetch FR and AR detail pages, merge into one document."""
    now = datetime.now(timezone.utc)
    doc = {"ad_id": ad_id, "scraped_at": now}
    raw_images = []

    # ── French detail page ─────────────────────────────────────────
    try:
        resp = session.get(fr_url, headers=HEADERS_FR, timeout=15)
        resp.raise_for_status()
        doc["url_fr"] = resp.url  # after redirect
        fr = parse_detail_page(BeautifulSoup(resp.text, "html.parser"))

        # Shared numeric/geo fields from FR (primary source)
        for f in ("price", "currency", "rooms", "bedrooms", "bathrooms",
                  "area_m2", "country", "latitude", "longitude"):
            if fr.get(f) is not None:
                doc[f] = fr[f]

        doc["title_fr"]         = fr.get("title", "")
        doc["description_fr"]   = fr.get("description", "")
        doc["city_fr"]          = fr.get("city", "")
        doc["location_text_fr"] = fr.get("location_text", "")
        doc["property_type_fr"] = fr.get("property_type")
        doc["features_fr"]      = fr.get("features", [])
        doc["main_features_fr"] = fr.get("main_features", {})
        doc["seller_name"]      = fr.get("seller_name", "")
        doc["seller_type"]      = fr.get("seller_type", "")

        raw_images = fr.get("raw_images", [])

        # Phone from JSON-LD (if present)
        if fr.get("phone_ld"):
            doc["phone"] = fr["phone_ld"]

    except Exception as e:
        log.warning(f"[ad {ad_id}] FR detail error: {e}")

    # ── Arabic detail page ─────────────────────────────────────────
    try:
        resp_ar = session.get(ar_url, headers=HEADERS_AR, timeout=15)
        resp_ar.raise_for_status()
        doc["url_ar"] = resp_ar.url
        ar = parse_detail_page(BeautifulSoup(resp_ar.text, "html.parser"))

        doc["title_ar"]         = ar.get("title", "")
        doc["description_ar"]   = ar.get("description", "")
        doc["city_ar"]          = ar.get("city", "")
        doc["location_text_ar"] = ar.get("location_text", "")
        doc["property_type_ar"] = ar.get("property_type")
        doc["features_ar"]      = ar.get("features", [])
        doc["main_features_ar"] = ar.get("main_features", {})

        # Coordinates fallback
        if not doc.get("latitude") and ar.get("latitude"):
            doc["latitude"]  = ar["latitude"]
            doc["longitude"] = ar["longitude"]

    except Exception as e:
        log.debug(f"[ad {ad_id}] AR detail error: {e}")

    # ── Phone via AJAX (if not already found) ─────────────────────
    if not doc.get("phone"):
        phone = get_phone(session, ad_id, doc.get("url_fr", fr_url))
        if phone:
            doc["phone"] = phone

    # ── Images → B2 ───────────────────────────────────────────────
    if raw_images:
        doc["images"]      = upload_images("mubawab", ad_id, raw_images)
        doc["image_count"] = len(doc["images"])

    return doc


# ══════════════════════════════════════════════════════════════════
# ZONE SCRAPING
# ══════════════════════════════════════════════════════════════════

def discover_zone(session, zone_name, fr_search, ar_search):
    """Return {ad_id: (fr_url, ar_url)} for all listings in a zone."""
    all_ids  = set()
    fr_urls  = {}
    ar_urls  = {}

    for lang, search_url in (("fr", fr_search), ("ar", ar_search)):
        consecutive_empty = 0
        for page in range(1, MAX_PAGES + 1):
            page_url = get_page_url(search_url, page)
            try:
                ids, fr, ar = get_ids_from_page(session, page_url, lang)
                if not ids:
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        break
                    time.sleep(DELAY)
                    continue

                consecutive_empty = 0
                new = ids - all_ids
                all_ids |= ids
                fr_urls.update(fr)
                ar_urls.update(ar)

                if new:
                    print(f"    {lang.upper()} p{page}: {len(new)} nouvelles ({len(all_ids)} total)")

            except Exception as e:
                log.warning(f"  [{zone_name}] {lang.upper()} p{page} error: {e}")

            time.sleep(DELAY)

    return all_ids, fr_urls, ar_urls


def scrape_zone(session, collection, zone_name, fr_search, ar_search):
    print(f"\n{'─'*60}")
    print(f"  Zone: {zone_name}")

    all_ids, fr_urls, ar_urls = discover_zone(session, zone_name, fr_search, ar_search)
    print(f"  Découverts: {len(all_ids)}")

    # Skip already in DB
    existing = {
        doc["ad_id"]
        for doc in collection.find(
            {"ad_id": {"$in": list(all_ids)}}, {"ad_id": 1, "_id": 0}
        )
    }
    new_ids = all_ids - existing
    print(f"  Nouveaux: {len(new_ids)}\n")

    success = errors = 0
    for i, ad_id in enumerate(sorted(new_ids), 1):
        fr_url = fr_urls.get(ad_id) or f"https://www.mubawab.tn/fr/pa/{ad_id}/"
        ar_url = ar_urls.get(ad_id) or fr_url.replace("/fr/pa/", "/ar/pa/")

        print(f"  [{i}/{len(new_ids)}] ad {ad_id}")
        try:
            doc = scrape_listing(session, ad_id, fr_url, ar_url)
            doc["zone"] = zone_name
            now = datetime.now(timezone.utc)

            collection.update_one(
                {"ad_id": ad_id},
                {
                    "$set":         {k: v for k, v in doc.items() if k != "first_seen"},
                    "$setOnInsert": {"first_seen": now},
                },
                upsert=True,
            )
            success += 1
            title = doc.get("title_fr") or doc.get("title_ar") or ""
            print(f"    ✅ {title[:60]}")

        except Exception as e:
            errors += 1
            log.warning(f"    ❌ ad {ad_id}: {e}")

        time.sleep(DELAY)

    return success, errors


# ══════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════

def scrape_all():
    print("\n" + "=" * 60)
    print("  MUBAWAB SCRAPER — Locations FR + AR")
    print("=" * 60)
    check_b2()
    print()

    collection = get_collection()
    session    = requests.Session()

    total_success = total_errors = 0
    for zone_name, fr_url, ar_url in ZONES:
        s, e = scrape_zone(session, collection, zone_name, fr_url, ar_url)
        total_success += s
        total_errors  += e

    print(f"\n{'='*60}")
    print(f"  ✅ Succès:  {total_success}")
    print(f"  ❌ Erreurs: {total_errors}")
    print(f"  Total en base: {collection.count_documents({})}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    scrape_all()
