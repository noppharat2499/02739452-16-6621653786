"""Media trends pipeline: TMDB + TVMaze + Jikan -> clean -> quality gate -> PostgreSQL."""
import json
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule

from media_pipeline.config import DATA_DIR
from media_pipeline.extract import (
    extract_jikan,
    extract_tmdb,
    extract_tvmaze,
    scrape_tvmaze_votes,
)
from media_pipeline.load import load, log_run
from media_pipeline.quality import run_checks
from media_pipeline.transform import transform

EXTRACT_IDS = ["extract.tmdb", "extract.jikan", "extract.tvmaze_api", "extract.tvmaze_scrape"]


@dag(
    dag_id="media_trends_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
    tags=["02739452", "media"],
)
def media_trends_pipeline():
    # ---- Extract: the sources run in parallel inside one TaskGroup -------------
    @task_group(group_id="extract")
    def extract():
        @task(task_id="tmdb")
        def tmdb(ds=None):
            return extract_tmdb(int(ds[:4]), ds)

        @task(task_id="jikan")
        def jikan(ds=None):
            return extract_jikan(int(ds[:4]), ds)

        @task(task_id="tvmaze_api")
        def tvmaze_api(ds=None):
            return extract_tvmaze(ds)

        @task(task_id="tvmaze_scrape")  # web scraping (requests + BeautifulSoup, robots.txt respected)
        def tvmaze_scrape(raw_path, ds=None):
            return scrape_tvmaze_votes(raw_path, ds)

        tmdb()
        jikan()
        tvmaze_scrape(tvmaze_api())

    # ---- Transform: runs even if some sources failed (all_done), needs >= 1 ----
    @task(task_id="transform", trigger_rule=TriggerRule.ALL_DONE)
    def transform_task(ds=None, ti=None):
        values = {tid: ti.xcom_pull(task_ids=tid) for tid in EXTRACT_IDS}
        raw = {
            "tmdb": values["extract.tmdb"],
            "jikan": values["extract.jikan"],
            "tvmaze": values["extract.tvmaze_api"],
        }
        if not any(raw.values()):
            raise AirflowFailException("every source failed, nothing to transform")
        return transform(raw, values["extract.tvmaze_scrape"], ds)

    # ---- Data quality gate: branch to load or quarantine ------------------------
    @task.branch(task_id="quality_gate")
    def quality_gate(stats, ti=None):
        report = run_checks(stats["path"])
        ti.xcom_push(key="report", value=report)
        return "load_to_postgres" if report["ok"] else "quarantine"

    @task(task_id="load_to_postgres")
    def load_to_postgres(stats):
        return load(stats["path"])

    @task(task_id="quarantine")
    def quarantine(ds=None, ti=None):
        report = ti.xcom_pull(task_ids="quality_gate", key="report")
        path = os.path.join(DATA_DIR, f"quarantine_{ds}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        return {"quarantine_file": path, "failed": [c for c in report["checks"] if not c["passed"]]}

    @task(task_id="log_run", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def log_run_task(ds=None, ti=None):
        loaded = ti.xcom_pull(task_ids="load_to_postgres")
        quarantined = ti.xcom_pull(task_ids="quarantine")
        stats = ti.xcom_pull(task_ids="transform") or {}
        report = ti.xcom_pull(task_ids="quality_gate", key="report")
        status = "loaded" if loaded else "quarantined"
        log_run(
            ti.run_id, ds, status, (loaded or {}).get("rows_loaded", 0),
            {"transform": stats, "quality": report, "load": loaded, "quarantine": quarantined},
        )

    ext = extract()
    stats = transform_task()
    ext >> stats
    gate = quality_gate(stats)
    loaded = load_to_postgres(stats)
    quarantined = quarantine()
    gate >> [loaded, quarantined]
    [loaded, quarantined] >> log_run_task()


media_trends_pipeline()
