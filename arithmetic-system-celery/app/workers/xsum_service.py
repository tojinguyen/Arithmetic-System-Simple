from ..celery import app
import logging

logger = logging.getLogger(__name__)


@app.task(name="xsum_task", queue="add_tasks")
def xsum_task(numbers: list[float]) -> float:
    if not isinstance(numbers, list):
        raise TypeError(f"numbers must be a list, got {type(numbers).__name__}")

    if not all(isinstance(i, (int, float)) for i in numbers):
        raise TypeError("All elements in numbers must be int or float.")

    try:
        return sum(numbers)
    except Exception as e:
        logger.error(f"Error in xsum task for input {numbers}: {e}")
        raise
