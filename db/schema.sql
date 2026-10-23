-- Schema for the "media" database (movies, series, anime from TMDB / TVMaze / Jikan)

CREATE TABLE IF NOT EXISTS media (
    id           SERIAL PRIMARY KEY,
    source       TEXT NOT NULL,              -- tmdb | tvmaze | jikan
    source_id    TEXT NOT NULL,
    media_type   TEXT NOT NULL,              -- movie | series | anime
    title        TEXT NOT NULL,
    title_norm   TEXT NOT NULL,
    release_date DATE,
    year         INT,
    url          TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_id)               -- target of the UPSERT
);

CREATE TABLE IF NOT EXISTS ratings (
    media_id    INT PRIMARY KEY REFERENCES media(id) ON DELETE CASCADE,
    score       NUMERIC(4,2),                -- normalised 0-10
    vote_count  INT,
    popularity  NUMERIC,
    followers   INT,                         -- jikan members / tvmaze followers (scraped)
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS genres (
    id   SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL                -- standardised genre
);

CREATE TABLE IF NOT EXISTS media_genres (
    media_id INT REFERENCES media(id) ON DELETE CASCADE,
    genre_id INT REFERENCES genres(id),
    PRIMARY KEY (media_id, genre_id)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          SERIAL PRIMARY KEY,
    run_id      TEXT,
    ds          DATE,
    status      TEXT,                        -- loaded | quarantined
    rows_loaded INT,
    details     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW v_media AS
SELECT m.id, m.source, m.source_id, m.media_type, m.title, m.release_date, m.year, m.url,
       r.score, r.vote_count, r.popularity, r.followers, r.fetched_at,
       COALESCE(string_agg(g.name, ', ' ORDER BY g.name), 'Other') AS genres
FROM media m
LEFT JOIN ratings r ON r.media_id = m.id
LEFT JOIN media_genres mg ON mg.media_id = m.id
LEFT JOIN genres g ON g.id = mg.genre_id
GROUP BY m.id, r.media_id;

CREATE OR REPLACE VIEW v_media_genre AS
SELECT m.id, m.source, m.media_type, m.title, m.year, g.name AS genre,
       r.score, r.vote_count, r.popularity, r.followers
FROM media m
JOIN media_genres mg ON mg.media_id = m.id
JOIN genres g ON g.id = mg.genre_id
LEFT JOIN ratings r ON r.media_id = m.id;
