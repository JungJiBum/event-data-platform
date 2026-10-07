"""Run: docker compose --env-file .env -f pipeline/docker-compose.yml exec -T airflow-scheduler python < tests/aggregation_checks.py"""
import copy
import unittest

import pendulum
from airflow.models import DagBag
from airflow.providers.postgres.hooks.postgres import PostgresHook


class AggregationChecks(unittest.TestCase):
    def test_date_boundaries_name_changes_and_rerun(self):
        dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
        self.assertEqual(dagbag.import_errors, {})
        task = dagbag.get_dag("daily_actor_stats").get_task("aggregate_actor_stats")
        conn = PostgresHook(postgres_conn_id="event_data").get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SET LOCAL TIME ZONE 'Asia/Seoul'")
                cursor.execute("""
                    CREATE TEMP TABLE processed_events (
                        event_id text, actor_id text, actor_name text, event_timestamp timestamptz
                    ) ON COMMIT DROP;
                    CREATE TEMP TABLE daily_actor_stats (
                        stat_date date, actor_id text, actor_name text, event_count integer,
                        PRIMARY KEY (stat_date, actor_id)
                    ) ON COMMIT DROP;
                    INSERT INTO processed_events VALUES
                        ('before', 'actor-1', 'Before', '2026-10-05 23:59:59+00'),
                        ('first', 'actor-1', 'Old Name', '2026-10-06 00:00:00+00'),
                        ('last', 'actor-1', 'New Name', '2026-10-06 23:59:59+00'),
                        ('next', 'actor-1', 'Next Day', '2026-10-07 00:00:00+00'),
                        ('other', 'actor-2', 'Other', '2026-10-06 12:00:00+00');
                """)
                def run(day):
                    rendered = copy.deepcopy(task)
                    interval = pendulum.parse(day + "T12:00:00Z")
                    rendered.render_template_fields({
                        "data_interval_start": interval,
                        "data_interval_end": interval.add(minutes=2),
                        "logical_date": interval,
                    })
                    self.assertNotIn("CURRENT_DATE", rendered.sql)
                    cursor.execute(rendered.sql)
                def rows():
                    cursor.execute("SELECT stat_date::text, actor_id, actor_name, event_count FROM daily_actor_stats ORDER BY stat_date, actor_id")
                    return cursor.fetchall()
                run("2026-10-06")
                expected = [("2026-10-06", "actor-1", "New Name", 2), ("2026-10-06", "actor-2", "Other", 1)]
                self.assertEqual(rows(), expected)
                run("2026-10-06")
                self.assertEqual(rows(), expected)
                cursor.execute("INSERT INTO processed_events VALUES ('late', 'actor-1', 'Late Arrival', '2026-10-06 23:59:59.5+00')")
                run("2026-10-06")
                expected[0] = ("2026-10-06", "actor-1", "Late Arrival", 3)
                self.assertEqual(rows(), expected)
                run("2026-10-07")
                self.assertEqual(rows(), expected + [("2026-10-07", "actor-1", "Next Day", 1)])
        finally:
            conn.rollback()
            conn.close()


if __name__ == "__main__":
    unittest.main()
