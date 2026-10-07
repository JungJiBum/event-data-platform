from datetime import datetime

from sqlalchemy import select

from models import RawEvent, ProcessedEvent
from task_queue import task_queue
from worker_database import WorkerSessionLocal

def get_raw_event(event_id: str) -> None:
    with WorkerSessionLocal() as session:
        result = session.execute(
            select(RawEvent).where(RawEvent.event_id == event_id)
        )

        raw_event = result.scalar_one_or_none()
        if raw_event is None:
            print(f"RAW_EVENT NOT FOUND: {event_id}")
            return
        raw_event.status = "PROCESSING"
        session.commit()

        payload = raw_event.payload
        processed_event = ProcessedEvent(
            event_id=payload["event_id"],
            actor_id=payload["actor"]["id"],
            actor_name=payload["actor"]["name"],
            verb=payload["verb"],
            object=payload["object"],
            properties=payload["data"],
            event_timestamp=datetime.fromisoformat(payload["timestamp"]),
        )
        session.add(processed_event)
        session.commit()

        raw_event.status = "COMPLETED"
        session.commit()


        print(f"RAW_EVENT: {raw_event.payload}")
        print(f"STATUS: {raw_event.status}")

@task_queue.task
def process_event(event_id: str) -> None:
    get_raw_event(event_id)