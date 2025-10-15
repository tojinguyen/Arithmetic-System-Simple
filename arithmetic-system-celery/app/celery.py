from celery import Celery

app = Celery(
    'arithmetic_system',
    broker='pyamqp://guest@rabbitmq//',
    backend='redis://redis:6379/0',
    include=[
        'app.workers.add_service',
        'app.workers.sub_service',
        'app.workers.mul_service',
        'app.workers.div_service',
        'app.workers.xsum_service',
        'app.workers.xprod_service'
    ]
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,
    task_soft_time_limit=25 * 60,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
)