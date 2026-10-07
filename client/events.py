import random
from datetime import datetime, timezone
from uuid import uuid4

def create_exercise_event(actor: dict, exercises: dict) -> dict:
    exercise_type = random.choice(["strength", "cardio"])
    exercise = random.choice(exercises[exercise_type])

    if exercise_type == "strength":
        data = {
            "weight": random.randrange(20, 101, 5),
            "reps": random.randint(5, 15),
        }
    else:
        data = {
            "duration": random.randint(300, 3600),
            "distance": round(random.uniform(1.0, 10.0), 2),
        }
    return {
        "event_id": str(uuid4()),
        "actor": actor,
        "verb": "exercise",
        "object": exercise["name"],
        "data": data,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

def create_rest_event(actor: dict) -> dict:
    return {
        "event_id": str(uuid4()),
        "actor": actor,
        "verb": "rest",
        "object": "rest",
        "data": {
            "duration": random.randint(30, 180),
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def create_event(actor: dict, exercises: dict) -> dict:
    verb = random.choice(["exercise", "rest"])

    if verb == "exercise":
        return create_exercise_event(actor, exercises)

    return create_rest_event(actor)