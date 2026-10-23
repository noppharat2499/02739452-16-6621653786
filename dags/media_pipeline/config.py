import os

DATA_DIR = os.getenv("PIPELINE_DATA_DIR", "/opt/airflow/data")
MIN_ROWS = int(os.getenv("MIN_ROWS", "80"))
SCRAPE_LIMIT = int(os.getenv("TVMAZE_SCRAPE_LIMIT", "40"))


def db_params():
    return dict(
        host=os.getenv("MEDIA_DB_HOST", "postgres"),
        port=int(os.getenv("MEDIA_DB_PORT", "5432")),
        dbname=os.getenv("MEDIA_DB_NAME", "media"),
        user=os.getenv("MEDIA_DB_USER", "airflow"),
        password=os.getenv("MEDIA_DB_PASSWORD", "airflow"),
    )
