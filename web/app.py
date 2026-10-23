"""Streamlit dashboard. Reads directly from PostgreSQL (no intermediate files)."""
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

st.set_page_config(page_title="Media Trends 2026", layout="wide")


@st.cache_resource
def engine():
    return create_engine(
        URL.create(
            "postgresql+psycopg2",
            username=os.getenv("MEDIA_DB_USER", "airflow"),
            password=os.getenv("MEDIA_DB_PASSWORD", "airflow"),
            host=os.getenv("MEDIA_DB_HOST", "localhost"),
            port=int(os.getenv("MEDIA_DB_PORT", "5432")),
            database=os.getenv("MEDIA_DB_NAME", "media"),
        )
    )


@st.cache_data(ttl=30)
def query(sql):
    return pd.read_sql(sql, engine())


st.title("🎬 Media Trends — Movies, Series & Anime")
st.caption("Sources: TMDB · TVMaze · Jikan (MyAnimeList) — loaded by an Apache Airflow DAG into PostgreSQL")

tab_dash, tab_status = st.tabs(["Dashboard", "Pipeline status"])

with tab_dash:
    try:
        df = query("SELECT * FROM v_media")
        dg = query("SELECT * FROM v_media_genre")
    except Exception as exc:  # DB not ready / tables missing
        st.error(f"Cannot read from PostgreSQL: {exc}")
        st.stop()

    if df.empty:
        st.info("No data yet. Open Airflow (http://localhost:8080) and trigger `media_trends_pipeline`.")
        st.stop()

    # ---- filters ----
    st.sidebar.header("Filters")
    sources = st.sidebar.multiselect("Source", sorted(df.source.unique()), default=sorted(df.source.unique()))
    types = st.sidebar.multiselect("Type", sorted(df.media_type.unique()), default=sorted(df.media_type.unique()))
    genre_opts = sorted(dg.genre.unique())
    genres = st.sidebar.multiselect("Genre", genre_opts)
    min_score = st.sidebar.slider("Minimum score", 0.0, 10.0, 0.0, 0.5)

    f = df[df.source.isin(sources) & df.media_type.isin(types) & (df.score.fillna(0) >= min_score)]
    fg = dg[dg.source.isin(sources) & dg.media_type.isin(types) & (dg.score.fillna(0) >= min_score)]
    if genres:
        ids = set(fg[fg.genre.isin(genres)].id)
        f, fg = f[f.id.isin(ids)], fg[fg.id.isin(ids) & fg.genre.isin(genres)]

    # ---- KPIs ----
    top_genre = fg.genre.value_counts().idxmax() if not fg.empty else "-"
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Titles", f"{len(f):,}")
    k2.metric("Average score", f"{f.score.mean():.2f}" if len(f) else "-")
    k3.metric("Total followers / members", f"{int(f.followers.fillna(0).sum()):,}")
    k4.metric("Top genre", top_genre)

    c1, c2 = st.columns(2)
    avg = f.groupby("source", as_index=False).agg(avg_score=("score", "mean"), titles=("id", "count"))
    c1.plotly_chart(
        px.bar(avg, x="source", y="avg_score", text_auto=".2f", color="source",
               title="Average score by source", range_y=[0, 10]),
        use_container_width=True,
    )
    share = fg.genre.value_counts().reset_index()
    share.columns = ["genre", "titles"]
    c2.plotly_chart(px.pie(share, names="genre", values="titles", title="Genre share"), use_container_width=True)

    c3, c4 = st.columns(2)
    gscore = fg.groupby("genre", as_index=False).agg(avg_score=("score", "mean"), titles=("id", "count"))
    c3.plotly_chart(
        px.bar(gscore.sort_values("avg_score"), x="avg_score", y="genre", orientation="h",
               text_auto=".2f", title="Average score by genre"),
        use_container_width=True,
    )
    sc = f.dropna(subset=["followers", "score"])
    sc = sc[sc.followers > 0]
    c4.plotly_chart(
        px.scatter(sc, x="followers", y="score", color="source", hover_name="title", log_x=True,
                   title="Score vs followers / members (log scale)"),
        use_container_width=True,
    )

    st.subheader("Top titles")
    n_posters = st.slider("Posters per type", 5, 50, 50, step=5)
    ptabs = st.tabs(["Movies", "Series", "Anime"])
    for ptab, mtype in zip(ptabs, ["movie", "series", "anime"]):
        with ptab:
            top = (f[f.media_type == mtype].dropna(subset=["image_url"])
                   .sort_values("score", ascending=False).head(n_posters))
            if top.empty:
                st.caption(f"No {mtype} posters yet (check the Source/Type filters or run the pipeline).")
                continue
            for start in range(0, len(top), 5):
                cols = st.columns(5)
                for col, (_, r) in zip(cols, top.iloc[start:start + 5].iterrows()):
                    col.image(r.image_url, use_container_width=True)
                    col.markdown(f"**{r.title}**  \n⭐ {r.score:.1f} · {r.source} · {r.genres}")

    st.subheader("Titles")
    st.dataframe(
        f.sort_values("score", ascending=False)[
            ["image_url", "title", "source", "media_type", "year", "genres", "score", "vote_count", "followers", "url"]
        ],
        use_container_width=True, hide_index=True, row_height=80,
        column_config={
            "url": st.column_config.LinkColumn("link"),
            "image_url": st.column_config.ImageColumn("poster", width="small"),
        },
    )

with tab_status:
    st.subheader("Pipeline runs")
    try:
        runs = query("SELECT created_at, run_id, status, rows_loaded, details FROM pipeline_runs ORDER BY id DESC LIMIT 20")
        counts = query("SELECT (SELECT count(*) FROM media) AS media, (SELECT count(*) FROM ratings) AS ratings, "
                       "(SELECT count(*) FROM genres) AS genres")
    except Exception as exc:
        st.error(f"Cannot read pipeline_runs: {exc}")
        st.stop()
    a, b, c = st.columns(3)
    a.metric("media rows", int(counts.media[0]))
    b.metric("ratings rows", int(counts.ratings[0]))
    c.metric("genres", int(counts.genres[0]))
    if runs.empty:
        st.info("No runs logged yet.")
    else:
        runs["quality_failed"] = runs.details.apply(
            lambda d: ", ".join(x["name"] for x in (d.get("quality") or {}).get("checks", []) if not x["passed"]) or "-"
        )
        runs["by_source"] = runs.details.apply(lambda d: str((d.get("transform") or {}).get("by_source", "")))
        st.dataframe(runs[["created_at", "run_id", "status", "rows_loaded", "by_source", "quality_failed"]],
                     use_container_width=True, hide_index=True)
