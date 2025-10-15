from ..celery import app
import logging

logger = logging.getLogger(__name__)


@app.task(name="divide_list_task", queue="div_tasks")
def divide_list_task(x: list[int | float]):
    if not isinstance(x, list):
        raise TypeError(f"Divide task expects a list, got {type(x).__name__}")

    if len(x) != 2:
        raise ValueError(f"Divide task expects 2 elements from chord, got {len(x)}")

    if x[1] == 0:
        raise ZeroDivisionError("Cannot divide by zero.")

    try:
        return x[0] / x[1]
    except Exception as e:
        logger.error(f"Error in division: {e}")
        raise
