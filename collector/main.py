from datetime import datetime
from typing import Annotated

from fastapi import FastAPI, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator
from prometheus_fastapi_instrumentator import Instrumentator
from sqlalchemy.dialects.postgresql import insert

from database import SessionLocal
from models import RawEvent
from tasks import process_event

NonEmptyString = Annotated[str, Field(strict=True, min_length=1)]


class Actor(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: NonEmptyString
    name: NonEmptyString


class Event(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_id: NonEmptyString
    actor: Actor
    verb: NonEmptyString
    object: NonEmptyString
    data: dict
    timestamp: str

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        # Validate timezone awareness while preserving the original timestamp string.
        parsed = datetime.fromisoformat(value)
        if parsed.utcoffset() is None:
            raise ValueError("timestamp must include a timezone offset")
        return value


app = FastAPI()
Instrumentator().instrument(app).expose(app)


@app.post("/events", status_code=status.HTTP_202_ACCEPTED)
async def collect_event(event: Event, response: Response) -> dict:
    payload = event.model_dump(mode="json")
    statement = (
        insert(RawEvent)
        .values(event_id=event.event_id, payload=payload, status="PENDING")
        .on_conflict_do_nothing(index_elements=[RawEvent.event_id])
        .returning(RawEvent.event_id)
    )
    async with SessionLocal() as session:
        inserted_id = (await session.execute(statement)).scalar_one_or_none()
        await session.commit()

    if inserted_id is None:
        response.status_code = status.HTTP_200_OK
        return {"status": "duplicate", "event_id": event.event_id}

    process_event.delay(event.event_id)
    return {"status": "accepted", "event_id": event.event_id}
