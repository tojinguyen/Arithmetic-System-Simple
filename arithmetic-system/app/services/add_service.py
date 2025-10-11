from ..celery import app
import logging
from celery import Task

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.task(name='add', queue='add_tasks')
def add(x, y):
    try:
        logger.info(f"Adding {x} + {y}")
        result = x + y
        logger.info(f"Result: {result}")
        return result
    except Exception as exc:
        logger.error(f"Error in add_task: {exc}")
        raise