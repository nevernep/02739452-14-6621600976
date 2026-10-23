
from datetime import datetime
import os
import json
import re

from airflow.decorators import dag, task, task_group
from airflow.utils.trigger_rule import TriggerRule
from src.pipeline import tmdb, omdb, wikipedia, normalize
import psycopg2


@dag(
    dag_id='movie_multi_source_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['movies', 'integration', 'cleaning']
)
def movie_pipeline():

    @task
    def fetch_tmdb():
        try:
            return {'items': tmdb(), 'error': None}
        except Exception as e:
            return {'items': [], 'error': str(e)}

    @task
    def fetch_wikipedia():
        try:
            return {'items': wikipedia(), 'error': None}
        except Exception as e:
            return {'items': [], 'error': str(e)}

    @task
    def fetch_omdb(tmdb_result):
        try:
            return {
                'items': omdb(tmdb_result['items']),
                'error': None
            }
        except Exception as e:
            return {'items': [], 'error': str(e)}

    @task_group(group_id='extract_sources')
    def extract():
        a = fetch_tmdb()
        b = fetch_wikipedia()
        c = fetch_omdb(a)
        return a, b, c

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def combine(a, b, c):
        results = {
            'tmdb': a,
            'wikipedia': b,
            'omdb': c
        }

        rows = normalize([
            item
            for result in results.values()
            for item in result['items']
        ])

        return {
            'rows': rows,
            'source_counts': {
                k: len(v['items'])
                for k, v in results.items()
            },
            'errors': {
                k: v['error']
                for k, v in results.items()
                if v['error']
            }
        }

    @task
    def clean_movies(result):
        gold = []
        quarantine = []

        for row in result['rows']:
            title = re.sub(
                r'\s+', ' ', str(row.get('title') or '')
            ).strip()

            year = row.get('release_year')
            sources = row.get('sources') or []

            row['title'] = title

            valid_structure = (
                bool(title)
                and isinstance(year, int)
                and 2020 <= year <= 2025
                and bool(row.get('movie_key'))
                and not re.fullmatch(r'\d+', title)
            )

            valid_values = (
                (row.get('tmdb_rating') is None or 0 <= row['tmdb_rating'] <= 10)
                and (row.get('imdb_rating') is None or 0 <= row['imdb_rating'] <= 10)
                and (row.get('runtime_minutes') is None or 1 <= row['runtime_minutes'] <= 600)
            )

            has_api_source = (
                'tmdb' in sources or 'omdb' in sources
            )

            if not valid_structure or not valid_values:
                row['quarantine_reason'] = 'invalid'
                quarantine.append(row)
            elif row.get('duplicate_count', 0) > 0:
                row['quarantine_reason'] = 'duplicate'
                quarantine.append(row)
            elif not has_api_source:
                row['quarantine_reason'] = 'suspicious'
                quarantine.append(row)
            else:
                gold.append(row)

        return {
            'gold': gold,
            'quarantine': quarantine,
            'source_counts': result['source_counts'],
            'errors': result['errors'],
            'cleaning_stats': {
                'input': len(result['rows']),
                'gold': len(gold),
                'quarantine': len(quarantine),
                'invalid': sum(r['quarantine_reason'] == 'invalid' for r in quarantine),
                'duplicates': sum(r['quarantine_reason'] == 'duplicate' for r in quarantine),
                'suspicious': sum(r['quarantine_reason'] == 'suspicious' for r in quarantine)
            }
        }

    @task.branch
    def quality(cleaned):
        rows = cleaned['gold']

        valid = (
            len(rows) >= 100
            and len({
                r['movie_key'] for r in rows
            }) == len(rows)
            and all(
                r['title']
                and 2020 <= r['release_year'] <= 2025
                for r in rows
            )
        )

        return 'upsert_movies' if valid else 'quality_failed'

    @task
    def upsert_movies(cleaned, **context):
        columns = (
            'movie_key', 'imdb_id', 'title',
            'release_year', 'genre', 'tmdb_rating',
            'imdb_rating', 'runtime_minutes', 'sources'
        )

        with psycopg2.connect(
            os.environ['APP_DB_DSN']
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    ALTER TABLE public.movies_quarantine
                    ADD COLUMN IF NOT EXISTS quarantine_reason TEXT
                """)

                # สร้าง snapshot ใหม่ในแต่ละรอบ
                # ไม่แก้ไข movies_raw_backup หรือ movies
                cur.execute(
                    'TRUNCATE TABLE public.movies_gold, '
                    'public.movies_quarantine'
                )

                for table, rows in (
                    ('movies_gold', cleaned['gold']),
                    ('movies_quarantine', cleaned['quarantine'])
                ):
                    insert_columns = columns + (
                        ('quarantine_reason',)
                        if table == 'movies_quarantine' else ()
                    )
                    query = f"""
                        INSERT INTO public.{table}
                        ({','.join(insert_columns)})
                        VALUES ({','.join(['%s'] * len(insert_columns))})
                        ON CONFLICT (movie_key) DO NOTHING
                    """

                    for row in rows:
                        cur.execute(
                            query,
                            tuple(row.get(k) for k in columns)
                            + ((row.get('quarantine_reason'),)
                               if table == 'movies_quarantine' else ())
                        )

                cur.execute("""
                    INSERT INTO public.pipeline_runs
                    (run_id, status, source_counts, row_count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (run_id)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        source_counts = EXCLUDED.source_counts,
                        row_count = EXCLUDED.row_count,
                        finished_at = now()
                """, (
                    context['run_id'],
                    'success',
                    json.dumps(cleaned['source_counts']),
                    len(cleaned['gold'])
                ))

    @task
    def quality_failed(cleaned, **context):
        with psycopg2.connect(
            os.environ['APP_DB_DSN']
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.pipeline_runs
                    (run_id, status, source_counts, row_count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (run_id)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        source_counts = EXCLUDED.source_counts,
                        row_count = EXCLUDED.row_count,
                        finished_at = now()
                """, (
                    context['run_id'],
                    'quality_failed',
                    json.dumps(cleaned['source_counts']),
                    len(cleaned['gold'])
                ))

        raise ValueError(
            'Data quality failed: fewer than '
            '100 valid Gold movies or invalid records'
        )

    a, b, c = extract()

    combined = combine(a, b, c)
    cleaned = clean_movies(combined)
    branch = quality(cleaned)

    good = upsert_movies.override(
        task_id='upsert_movies'
    )(cleaned)

    bad = quality_failed.override(
        task_id='quality_failed'
    )(cleaned)

    branch >> [good, bad]


movie_pipeline()
