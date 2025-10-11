from ..celery import app
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.task(name='divide', queue='div_tasks')
def divide(x, y):
    try:
        logger.info(f"Dividing {x} by {y}")
        if y == 0:
            raise ValueError("Cannot divide by zero.")
        result = x / y
        logger.info(f"Result of division: {result}")
        return result
    except Exception as e:
        logger.error(f"Error in division: {e}")
        raise
