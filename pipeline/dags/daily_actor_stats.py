from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="daily_actor_stats",
    start_date=datetime(2026, 10, 7, tzinfo=timezone.utc),
    schedule="*/2 * * * *",
    catchup=False,
    tags=["event-data-platform"],
) as dag:
    aggregate_actor_stats = SQLExecuteQueryOperator(
        task_id="aggregate_actor_stats",
        conn_id="event_data",
        sql="""
            INSERT INTO daily_actor_stats (
                stat_date,
                actor_id,
                actor_name,
                event_count
            )
            SELECT
                DATE '{{ data_interval_start.in_timezone("UTC").to_date_string() }}' AS stat_date,
                actor_id,
                (ARRAY_AGG(actor_name ORDER BY event_timestamp DESC, event_id DESC))[1] AS actor_name,
                COUNT(*) AS event_count
            FROM processed_events
            WHERE event_timestamp >=
                TIMESTAMPTZ '{{ data_interval_start.in_timezone("UTC").to_date_string() }} 00:00:00+00'
              AND event_timestamp <
                TIMESTAMPTZ '{{ data_interval_start.in_timezone("UTC").to_date_string() }} 00:00:00+00'
                + INTERVAL '1 day'
            GROUP BY actor_id
            ON CONFLICT (stat_date, actor_id)
            DO UPDATE SET
                actor_name = EXCLUDED.actor_name,
                event_count = EXCLUDED.event_count;
        """,
    )
