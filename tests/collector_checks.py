"""Run: docker compose exec -T collector python < tests/collector_checks.py"""
import asyncio
import json
import unittest
from unittest.mock import Mock
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import sessionmaker

import main
import tasks
from database import engine as async_engine
from models import Base, RawEvent, ProcessedEvent
from worker_database import engine as sync_engine


class CollectorChecks(unittest.TestCase):
    def test_validation_duplicates_and_worker(self):
        schema = "portfolio_test_" + uuid4().hex
        with sync_engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        test_sync = sync_engine.execution_options(schema_translate_map={None: schema})
        test_async = async_engine.execution_options(schema_translate_map={None: schema})
        original_session = main.SessionLocal
        original_worker_session = tasks.WorkerSessionLocal
        original_delay = main.process_event.delay
        try:
            Base.metadata.create_all(test_sync)
            main.SessionLocal = async_sessionmaker(test_async, expire_on_commit=False)
            tasks.WorkerSessionLocal = sessionmaker(test_sync, expire_on_commit=False)
            main.process_event.delay = Mock()
            event = {
                "event_id": str(uuid4()),
                "actor": {"id": "test-actor", "name": "Test Actor", "extra": "retained"},
                "verb": "exercise", "object": "squat", "data": {"reps": 10},
                "timestamp": "2026-10-07T12:00:00Z", "extra": "retained",
            }

            async def post(payload):
                messages = []
                body = json.dumps(payload).encode()
                async def receive():
                    return {"type": "http.request", "body": body, "more_body": False}
                async def send(message):
                    messages.append(message)
                await main.app({
                    "type": "http", "asgi": {"version": "3.0"},
                    "http_version": "1.1", "method": "POST", "scheme": "http",
                    "path": "/events", "raw_path": b"/events", "query_string": b"",
                    "root_path": "", "headers": [(b"content-type", b"application/json")],
                    "client": ("127.0.0.1", 1234), "server": ("test", 8000),
                }, receive, send)
                status = next(m["status"] for m in messages if m["type"] == "http.response.start")
                result = json.loads(b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body"))
                return status, result

            async def checks():
                invalid = [
                    {}, {**event, "event_id": ""}, {**event, "event_id": 123},
                    {**event, "actor": {"id": "a"}}, {**event, "data": []},
                    {**event, "timestamp": "invalid"}, {**event, "timestamp": "123"},
                    {**event, "timestamp": "2026-10-07T12:00:00"},
                ]
                for payload in invalid:
                    self.assertEqual((await post(payload))[0], 422, payload)
                self.assertEqual((await post(event))[0], 202)
                changed = {**event, "verb": "rest"}
                code, body = await post(changed)
                self.assertEqual(code, 200)
                self.assertEqual(body["status"], "duplicate")
                concurrent = {**event, "event_id": str(uuid4())}
                results = await asyncio.gather(post(concurrent), post(concurrent))
                self.assertEqual(sorted(r[0] for r in results), [200, 202])
                self.assertEqual(main.process_event.delay.call_count, 2)
                await test_async.dispose()
            asyncio.run(checks())
            with tasks.WorkerSessionLocal() as session:
                raw = session.get(RawEvent, event["event_id"])
                self.assertEqual(raw.payload, event)
                self.assertEqual(raw.status, "PENDING")
            tasks.get_raw_event(event["event_id"])
            with tasks.WorkerSessionLocal() as session:
                self.assertEqual(session.get(RawEvent, event["event_id"]).status, "COMPLETED")
                processed = session.get(ProcessedEvent, event["event_id"])
                self.assertEqual(processed.actor_id, "test-actor")
                self.assertEqual(processed.properties, event["data"])
        finally:
            main.SessionLocal = original_session
            tasks.WorkerSessionLocal = original_worker_session
            main.process_event.delay = original_delay
            with sync_engine.begin() as conn:
                conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))


if __name__ == "__main__":
    unittest.main()
