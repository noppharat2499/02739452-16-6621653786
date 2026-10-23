"""Extractors: TMDB (API), Jikan (API), TVMaze (API + HTML scraping)."""
import json
import logging
import os
import re
import time
import urllib.robotparser
from datetime import date, timedelta

import requests
from bs4 import BeautifulSoup

from .config import DATA_DIR, SCRAPE_LIMIT

log = logging.getLogger(__name__)
UA = "KU-02739452-student-project/1.0 (educational, no personal data collected)"


def _get(url, params=None, retries=4):
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers={"User-Agent": UA}, timeout=30)
            if r.status_code == 429:  # rate limited -> back off and retry
                time.sleep(2 * (i + 1))
                last = RuntimeError(f"429 from {url}")
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            last = exc
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"GET {url} failed after {retries} tries: {last}")


def _save(name, payload, ds):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, f"{name}_{ds}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    return path


# ----------------------------------------------------------------- TMDB (API)
def extract_tmdb(year, ds, per_type=None):
    per_type = per_type or {"movie": 55, "tv": 30}
    key = os.environ["TMDB_API_KEY"]  # KeyError -> task fails visibly
    base = "https://api.themoviedb.org/3"
    genre_names = {}
    for kind in ("movie", "tv"):
        data = _get(f"{base}/genre/{kind}/list", {"api_key": key, "language": "en-US"}).json()
        for g in data["genres"]:
            genre_names[g["id"]] = g["name"]

    rows = []
    for kind, media_type, year_param in (
        ("movie", "movie", "primary_release_year"),
        ("tv", "series", "first_air_date_year"),
    ):
        got = []
        page = 1
        want = per_type[kind]
        while len(got) < want and page <= 5:
            data = _get(
                f"{base}/discover/{kind}",
                {
                    "api_key": key,
                    "language": "en-US",
                    "sort_by": "popularity.desc",
                    year_param: year,
                    "include_adult": "false",
                    "page": page,
                },
            ).json()
            for it in data.get("results", []):
                got.append(
                    {
                        "source": "tmdb",
                        "source_id": f"{kind}-{it['id']}",
                        "media_type": media_type,
                        "title": it.get("title") or it.get("name"),
                        "release_date": it.get("release_date") or it.get("first_air_date"),
                        "score": it.get("vote_average"),
                        "vote_count": it.get("vote_count"),
                        "popularity": it.get("popularity"),
                        "genres": [genre_names.get(i, "Other") for i in it.get("genre_ids", [])],
                        "url": f"https://www.themoviedb.org/{kind}/{it['id']}",
                        "image_url": (f"https://image.tmdb.org/t/p/w342{it['poster_path']}"
                                      if it.get("poster_path") else None),
                    }
                )
            if page >= data.get("total_pages", 1):
                break
            page += 1
            time.sleep(0.3)
        rows += got[:want]
    log.info("TMDB rows: %d", len(rows))
    return _save("raw_tmdb", rows, ds)


# ---------------------------------------------------------------- Jikan (API)
def extract_jikan(year, ds, pages=3, keep=55):
    rows, seen = [], set()
    for page in range(1, pages + 1):
        data = _get(
            "https://api.jikan.moe/v4/anime",
            {
                "start_date": f"{year}-01-01",
                "end_date": f"{year}-12-31",
                "order_by": "members",
                "sort": "desc",
                "sfw": "true",
                "limit": 25,
                "page": page,
            },
        ).json()
        for it in data.get("data", []):
            if it["mal_id"] in seen:
                continue
            seen.add(it["mal_id"])
            rows.append(
                {
                    "source": "jikan",
                    "source_id": str(it["mal_id"]),
                    "media_type": "anime",
                    "title": it.get("title_english") or it.get("title"),
                    "release_date": (it.get("aired") or {}).get("from"),
                    "score": it.get("score"),
                    "vote_count": it.get("scored_by"),
                    "popularity": it.get("members"),
                    "followers": it.get("members"),
                    "genres": [g["name"] for g in (it.get("genres") or [])],
                    "url": it.get("url"),
                    "image_url": ((it.get("images") or {}).get("jpg") or {}).get("image_url"),
                }
            )
        if not data.get("pagination", {}).get("has_next_page"):
            break
        time.sleep(1.1)  # Jikan limit: ~3 req/s, 60 req/min
    rows = rows[:keep]
    log.info("Jikan rows: %d", len(rows))
    return _save("raw_jikan", rows, ds)


# --------------------------------------------------------------- TVMaze (API)
def extract_tvmaze(ds, days=7, top=30):
    end = date.fromisoformat(ds)
    shows = {}
    for back in range(days):
        d = (end - timedelta(days=back)).isoformat()
        for url, params in (
            ("https://api.tvmaze.com/schedule", {"country": "US", "date": d}),
            ("https://api.tvmaze.com/schedule/web", {"date": d}),
        ):
            for ep in _get(url, params).json():
                show = ep.get("show") or (ep.get("_embedded") or {}).get("show")
                if show:
                    shows[show["id"]] = show
            time.sleep(0.6)  # TVMaze limit: 20 calls / 10 s

    rows = []
    for s in shows.values():
        avg = (s.get("rating") or {}).get("average")
        if avg is None:
            continue
        rows.append(
            {
                "source": "tvmaze",
                "source_id": str(s["id"]),
                "media_type": "series",
                "title": s.get("name"),
                "release_date": s.get("premiered"),
                "score": avg,
                "vote_count": None,
                "popularity": s.get("weight"),
                "genres": s.get("genres") or [],
                "url": s.get("url"),
                "image_url": (s.get("image") or {}).get("medium"),
            }
        )
    rows.sort(key=lambda r: -(r["popularity"] or 0))
    rows = rows[:top]
    log.info("TVMaze rows: %d", len(rows))
    return _save("raw_tvmaze", rows, ds)


# ------------------------------------------------- TVMaze (web scraping, HTML)
def _to_int(text):
    m = re.fullmatch(r"([\d.,]+)\s*([kKmM]?)", text.strip())
    if not m:
        return None
    num = float(m.group(1).replace(",", ""))
    mult = {"": 1, "k": 1_000, "m": 1_000_000}[m.group(2).lower()]
    return int(num * mult)


def parse_votes(html):
    """TVMaze show pages display the rating as e.g. '6.4 (52 votes)'.
    Returns the vote count, or None when the pattern is not found."""
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    for pat in (
        r"\(\s*([\d.,]+\s*[kKmM]?)\s+votes?\s*\)",
        r"([\d.,]+\s*[kKmM]?)\s+votes?\b",
    ):
        m = re.search(pat, text, re.I)
        if m:
            val = _to_int(m.group(1))
            if val is not None:
                return val
    return None


def scrape_tvmaze_votes(raw_path, ds, limit=None):
    limit = limit or SCRAPE_LIMIT
    with open(raw_path, encoding="utf-8") as f:
        shows = json.load(f)

    rp = urllib.robotparser.RobotFileParser()  # obey robots.txt
    rp.parse(_get("https://www.tvmaze.com/robots.txt").text.splitlines())

    votes, tried = {}, 0
    for s in shows[:limit]:
        url = s["url"]
        if not rp.can_fetch(UA, url):
            log.warning("robots.txt disallows %s", url)
            continue
        tried += 1
        val = parse_votes(_get(url).text)
        if val is not None:
            votes[s["source_id"]] = val
        time.sleep(1.0)  # be polite

    if tried and not votes:
        raise RuntimeError("scraped pages but found no vote counts: update parse_votes()")
    log.info("scraped vote counts for %d/%d shows", len(votes), tried)
    return _save("scrape_tvmaze", votes, ds)
