"""Idempotent load into PostgreSQL using UPSERT (ON CONFLICT)."""
import json

import psycopg2
from psycopg2.extras import Json, execute_values

from .config import db_params


def load(path):
    with open(path, encoding="utf-8") as f:
        rows = json.load(f)

    conn = psycopg2.connect(**db_params())
    try:
        with conn, conn.cursor() as cur:
            # 1) media: UPSERT on (source, source_id)
            got = execute_values(
                cur,
                """
                INSERT INTO media (source, source_id, media_type, title, title_norm, release_date, year, url, image_url)
                VALUES %s
                ON CONFLICT (source, source_id) DO UPDATE SET
                    media_type = EXCLUDED.media_type, title = EXCLUDED.title,
                    title_norm = EXCLUDED.title_norm, release_date = EXCLUDED.release_date,
                    year = EXCLUDED.year, url = EXCLUDED.url, image_url = COALESCE(EXCLUDED.image_url, media.image_url),
                    updated_at = now()
                RETURNING id, source, source_id
                """,
                [(r["source"], r["source_id"], r["media_type"], r["title"], r["title_norm"],
                  r["release_date"], r["year"], r["url"], r.get("image_url")) for r in rows],
                page_size=1000,
                fetch=True,
            )
            ids = {(s, sid): i for i, s, sid in got}

            # keep only the current top-N for every source that took part in this run
            # (a source that failed is not in `rows`, so its older data is left untouched)
            for src in {r["source"] for r in rows}:
                keep = [i for (s, _), i in ids.items() if s == src]
                cur.execute("DELETE FROM media WHERE source = %s AND id <> ALL(%s)", (src, keep))

            # 2) ratings: UPSERT on media_id (keep old followers if the scraper missed this run)
            execute_values(
                cur,
                """
                INSERT INTO ratings (media_id, score, vote_count, popularity, followers, fetched_at)
                VALUES %s
                ON CONFLICT (media_id) DO UPDATE SET
                    score = EXCLUDED.score, vote_count = EXCLUDED.vote_count,
                    popularity = EXCLUDED.popularity,
                    followers = COALESCE(EXCLUDED.followers, ratings.followers),
                    fetched_at = now()
                """,
                [(ids[(r["source"], r["source_id"])], r["score"], r["vote_count"],
                  r["popularity"], r["followers"]) for r in rows],
                template="(%s, %s, %s, %s, %s, now())",
                page_size=1000,
            )

            # 3) genres + links
            names = sorted({g for r in rows for g in r["genres"]})
            execute_values(cur, "INSERT INTO genres (name) VALUES %s ON CONFLICT (name) DO NOTHING",
                           [(n,) for n in names])
            cur.execute("SELECT name, id FROM genres WHERE name = ANY(%s)", (names,))
            gid = dict(cur.fetchall())
            media_ids = list(ids.values())
            cur.execute("DELETE FROM media_genres WHERE media_id = ANY(%s)", (media_ids,))
            execute_values(
                cur,
                "INSERT INTO media_genres (media_id, genre_id) VALUES %s ON CONFLICT DO NOTHING",
                [(ids[(r["source"], r["source_id"])], gid[g]) for r in rows for g in r["genres"]],
                page_size=2000,
            )

            cur.execute("SELECT count(*) FROM media")
            total = cur.fetchone()[0]
        return {"rows_loaded": len(rows), "media_total": total}
    finally:
        conn.close()


def log_run(run_id, ds, status, rows_loaded, details):
    conn = psycopg2.connect(**db_params())
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline_runs (run_id, ds, status, rows_loaded, details) VALUES (%s, %s, %s, %s, %s)",
                (run_id, ds, status, rows_loaded, Json(details)),
            )
    finally:
        conn.close()
