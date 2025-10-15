from ..celery import app
from math import prod
import logging

logger = logging.getLogger(__name__)


@app.task(name="xprod_task", queue="mul_tasks")
def xprod_task(numbers):
    if not isinstance(numbers, list):
        raise TypeError(f"numbers must be a list, got {type(numbers).__name__}")

    if not all(isinstance(i, (int, float)) for i in numbers):
        raise TypeError("All elements in numbers must be int or float.")
    try:
        return prod(numbers)
    except Exception as e:
        logger.error(f"Error in xprod task for input {numbers}: {e}")
        raise
