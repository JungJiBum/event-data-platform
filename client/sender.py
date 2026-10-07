import httpx


async def send_event(
    client: httpx.AsyncClient,
    collector_url: str,
    event: dict,
) -> None:
    actor = event["actor"]["name"]
    verb = event["verb"]
    object_ = event["object"]

    try:
        response = await client.post(
            collector_url,
            json=event,
        )

        print(
            f"[{actor}] {verb} | {object_} "
            f"→ POST {response.status_code}"
        )

    except httpx.RequestError as exc:
        print(
            f"[{actor}] {verb} | {object_} "
            f"→ POST FAILED: {exc}"
        )