import json
from pathlib import Path
from typing import Any

SAMPLE_DATA_DIR = Path(__file__).parent / "sample_data"

def load_json(filename: str) -> Any:
    path = SAMPLE_DATA_DIR / filename

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)

def load_actors() -> list[dict]:
    return load_json("actors.json")

def load_exercises() -> dict:
    return load_json("exercises.json")
