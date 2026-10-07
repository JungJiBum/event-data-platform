import asyncio
import os

import httpx

from events import create_event
from loader import load_actors, load_exercises
from sender import send_event


COLLECTOR_URL = os.getenv(
    "COLLECTOR_URL",
    "http://collector:8000/events",
)

EVENT_INTERVAL = float(
    os.getenv("EVENT_INTERVAL", "5")
)


async def run_actor(
    actor: dict,
    exercises: dict,
    client: httpx.AsyncClient,
) -> None:
    while True:
        event = create_event(actor, exercises)

        await send_event(
            client,
            COLLECTOR_URL,
            event,
        )

        await asyncio.sleep(EVENT_INTERVAL)


async def main() -> None:
    actors = load_actors()
    exercises = load_exercises()

    async with httpx.AsyncClient() as client:
        await asyncio.gather(
            *[
                run_actor(actor, exercises, client)
                for actor in actors
            ]
        )


if __name__ == "__main__":
    asyncio.run(main())