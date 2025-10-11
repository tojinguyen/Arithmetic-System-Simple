from ..celery import app
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.task(name='multiply', queue='mul_tasks')
def multiply(x, y):
    try:
        logger.info(f"Multiplying {x} * {y}")
        result = x * y
        logger.info(f"Result: {result}")
        return result
    except Exception as exc:
        logger.error(f"Error in multiply_task: {exc}")
        raise
