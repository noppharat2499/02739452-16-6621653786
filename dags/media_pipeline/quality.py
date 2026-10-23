"""Data quality gate run between transform and load."""
import json

from .config import MIN_ROWS


def run_checks(path, min_rows=None):
    min_rows = MIN_ROWS if min_rows is None else min_rows
    with open(path, encoding="utf-8") as f:
        rows = json.load(f)

    keys = [(r["source"], r["source_id"]) for r in rows]
    with_year = sum(1 for r in rows if r.get("year"))
    checks = [
        ("min_rows", len(rows) >= min_rows, f"{len(rows)} rows (need >= {min_rows})"),
        ("no_null_keys", all(r.get("title") and r.get("source_id") for r in rows), "title/source_id present"),
        ("score_in_range", all(r.get("score") is not None and 0 <= r["score"] <= 10 for r in rows), "0 <= score <= 10"),
        ("no_duplicate_keys", len(keys) == len(set(keys)), f"{len(keys) - len(set(keys))} duplicate (source, id)"),
        ("year_coverage", bool(rows) and with_year / len(rows) >= 0.8, f"{with_year}/{len(rows)} rows have a year"),
        ("genre_present", all(r.get("genres") for r in rows), "every row has a genre"),
    ]
    return {
        "ok": all(c[1] for c in checks),
        "checks": [{"name": n, "passed": bool(p), "detail": d} for n, p, d in checks],
    }
