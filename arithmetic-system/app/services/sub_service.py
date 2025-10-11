from ..celery import app
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.task(name='subtract', queue='sub_tasks')
def subtract(x, y):
    try:
        logger.info(f"Subtracting {x} - {y}")
        result = x - y
        logger.info(f"Result: {result}")
        return result
    except Exception as exc:
        logger.error(f"Error in subtract_task: {exc}")
        raise

