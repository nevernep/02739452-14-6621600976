# Movie Data Integration — Airflow + PostgreSQL + Streamlit

Educational multi-source movie pipeline (2020–2025): TMDB API, OMDb API, Wikipedia HTML scraping.

## Start
1. Copy `.env.example` to `.env`; replace API keys and passwords.
2. Check Wikipedia robots.txt and applicable website terms before scraping. Update the User-Agent contact in `src/pipeline.py`.
3. Run `docker compose up airflow-init` (first time), then `docker compose up -d`.
4. Open Airflow at http://localhost:8080, sign in as `airflow` using `AIRFLOW_ADMIN_PASSWORD`, and trigger `movie_multi_source_pipeline`.
5. Open dashboard at http://localhost:8501.

## Data cleaning and quality rules
- `movies_gold` contains records with a valid title, a release year from 2020–2025, valid rating/runtime ranges, and TMDB or OMDb support.
- `movies_quarantine` keeps rejected records with `quarantine_reason`: `invalid`, `duplicate`, or `suspicious`.
- Duplicate detection applies within the same source for the normalized title/year key; matching records from different sources are merged and retained.
- The quality gate requires at least 100 unique Gold movies before the snapshot is written.

## Dashboard
- **Overview**: KPI cards and release-year/source charts.
- **Movie Explorer**: filters for year, source, genre, and title plus a movie table.
- **Analytics**: rating and genre analysis.
- **Pipeline Status**: recent runs, Gold/Quarantine KPIs, and counts of invalid, duplicate, and suspicious records.

## Important caveats
- The Wikipedia HTML parser is best-effort and must be tested against the live site.
- OMDb enrichment depends on TMDB seed results, so OMDb is not fully independent. For strict parallel-source grading, refactor to use independent movie title seeds.
- Deduplication uses normalized title + year, which may not match alternative titles across sites; improve with IMDb IDs.
- The pipeline has not been integration-tested against live APIs or Docker in this environment.
- `.env` is ignored; never commit secrets.
