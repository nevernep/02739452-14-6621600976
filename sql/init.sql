CREATE TABLE IF NOT EXISTS movies (
 movie_key TEXT PRIMARY KEY, imdb_id TEXT, title TEXT NOT NULL, release_year INTEGER,
 genre TEXT, tmdb_rating NUMERIC, imdb_rating NUMERIC, runtime_minutes INTEGER,
 sources TEXT[] NOT NULL DEFAULT '{}', updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
 run_id TEXT PRIMARY KEY, status TEXT NOT NULL, source_counts JSONB NOT NULL DEFAULT '{}',
 row_count INTEGER NOT NULL DEFAULT 0, finished_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS movies_gold (
 movie_key TEXT PRIMARY KEY, imdb_id TEXT, title TEXT NOT NULL,
 release_year INTEGER, genre TEXT, tmdb_rating NUMERIC,
 imdb_rating NUMERIC, runtime_minutes INTEGER,
 sources TEXT[] NOT NULL DEFAULT '{}',
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS movies_quarantine (
 movie_key TEXT PRIMARY KEY, imdb_id TEXT, title TEXT NOT NULL,
 release_year INTEGER, genre TEXT, tmdb_rating NUMERIC,
 imdb_rating NUMERIC, runtime_minutes INTEGER,
 sources TEXT[] NOT NULL DEFAULT '{}',
 quarantine_reason TEXT NOT NULL DEFAULT 'suspicious',
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
