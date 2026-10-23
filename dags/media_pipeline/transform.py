"""Clean, standardise genres/dates and drop cross-source duplicates."""
import json
import os
import re

import pandas as pd

from .config import DATA_DIR

AA, SF, MT, FK = "Action/Adventure", "Sci-Fi", "Mystery/Thriller", "Family/Kids"
GENRE_MAP = {
    "action": AA, "adventure": AA, "action & adventure": AA, "western": AA,
    "comedy": "Comedy",
    "drama": "Drama", "war": "Drama", "war & politics": "Drama", "history": "Drama",
    "soap": "Drama", "slice of life": "Drama",
    "fantasy": "Fantasy", "supernatural": "Fantasy",
    "science fiction": SF, "sci-fi": SF, "sci-fi & fantasy": SF,
    "horror": "Horror",
    "romance": "Romance",
    "animation": "Animation",
    "crime": "Crime",
    "mystery": MT, "thriller": MT, "suspense": MT,
    "documentary": "Documentary", "news": "Documentary",
    "family": FK, "kids": FK,
}
# final size per (source, type): 50 movies, 50 series (25 TMDB + 25 TVMaze), 50 anime
LIMITS = {("tmdb", "movie"): 50, ("tmdb", "series"): 25, ("tvmaze", "series"): 25, ("jikan", "anime"): 50}
PRIORITY = {"tmdb": 0, "tvmaze": 1, "jikan": 2}  # which copy wins when a title is in several sources


def map_genres(names):
    out = []
    for n in names or []:
        g = GENRE_MAP.get(str(n).strip().lower(), "Other")
        if g not in out:
            out.append(g)
    if len(out) > 1 and "Other" in out:
        out.remove("Other")
    return out or ["Other"]


def _norm_title(t):
    return re.sub(r"[^0-9a-z฀-๿]+", "", t.lower())


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f


def _int(v):
    f = _num(v)
    return None if f is None else int(f)


def transform(raw_paths, scrape_path, ds):
    recs = []
    for source, path in raw_paths.items():
        if path:
            with open(path, encoding="utf-8") as f:
                recs += json.load(f)
    scraped = {}  # tvmaze source_id -> vote count scraped from the HTML page
    if scrape_path and os.path.exists(scrape_path):
        with open(scrape_path, encoding="utf-8") as f:
            scraped = json.load(f)

    stats = {"rows_in": len(recs), "dropped_invalid": 0, "dropped_no_score": 0, "cross_source_duplicates": 0}
    clean = []
    for r in recs:
        title = (r.get("title") or "").strip()
        sid = str(r.get("source_id") or "").strip()
        if not title or not sid:
            stats["dropped_invalid"] += 1
            continue
        score, votes = _num(r.get("score")), _num(r.get("vote_count"))
        if votes is None and r["source"] == "tvmaze":
            votes = _num(scraped.get(sid))
        if score is None or (score == 0 and not votes):
            stats["dropped_no_score"] += 1
            continue
        d = pd.to_datetime(r.get("release_date"), errors="coerce", utc=True)
        fol = _int(r.get("followers"))
        clean.append(
            {
                "source": r["source"],
                "source_id": sid,
                "media_type": r["media_type"],
                "title": title,
                "title_norm": _norm_title(title),
                "release_date": None if pd.isna(d) else str(d.date()),
                "year": None if pd.isna(d) else int(d.year),
                "url": r.get("url"),
                "image_url": r.get("image_url"),
                "score": round(min(max(score, 0), 10), 2),
                "vote_count": _int(votes),
                "popularity": _num(r.get("popularity")),
                "followers": fol,
                "genres": map_genres(r.get("genres")),
            }
        )

    # cross-source de-duplication: same normalised title + year seen in another source
    clean.sort(key=lambda x: (PRIORITY.get(x["source"], 9), -(x["popularity"] or 0)))
    seen, final = {}, []
    for x in clean:
        key = (x["title_norm"], x["year"]) if x["year"] else None
        if key and key in seen and seen[key] != x["source"]:
            stats["cross_source_duplicates"] += 1
            continue
        if key:
            seen.setdefault(key, x["source"])
        final.append(x)

    counts, capped = {}, []
    for x in final:  # already ordered by popularity within each source
        k = (x["source"], x["media_type"])
        if counts.get(k, 0) < LIMITS.get(k, 50):
            counts[k] = counts.get(k, 0) + 1
            capped.append(x)
    final = capped

    stats["rows_out"] = len(final)
    stats["by_source"] = {s: sum(1 for x in final if x["source"] == s) for s in PRIORITY}
    stats["by_type"] = {m: sum(1 for x in final if x["media_type"] == m) for m in ("movie", "series", "anime")}
    os.makedirs(DATA_DIR, exist_ok=True)
    out = os.path.join(DATA_DIR, f"clean_{ds}.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(final, f, ensure_ascii=False)
    stats["path"] = out
    return stats
