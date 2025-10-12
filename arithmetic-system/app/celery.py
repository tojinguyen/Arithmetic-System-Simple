from celery import Celery

app = Celery(
    'arithmetic_system',
    broker='pyamqp://guest@rabbitmq//',
    backend='redis://redis:6379/0',
    include=[
        'app.services.add_service',
        'app.services.sub_service',
        'app.services.mul_service',
        'app.services.div_service',
        'app.services.xsum_service',
        'app.services.xprod_service',
        'app.services.combiner_service'
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