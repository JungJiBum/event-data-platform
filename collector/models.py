from datetime import date, datetime

from sqlalchemy import Date, DateTime, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class RawEvent(Base):
    __tablename__ = "raw_events"

    event_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
    )

    payload: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String,
        nullable=False,
        default="PENDING",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
    )

    actor_id: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    actor_name: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    verb: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    object: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )
    properties: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
    )

    event_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

class DailyActorStats(Base):
    __tablename__ = "daily_actor_stats"

    stat_date: Mapped[date] = mapped_column(
        Date,
        primary_key=True,
    )

    actor_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
    )

    actor_name: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    event_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )