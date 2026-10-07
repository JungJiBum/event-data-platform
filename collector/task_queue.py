from celery import Celery

task_queue = Celery(
    "task_queue",
    broker="pyamqp://guest:guest@rabbitmq:5672//",
    include=["tasks"],
)