from ..celery import app
import logging

logger = logging.getLogger(__name__)


@app.task(name="subtract_list_task", queue="sub_tasks")
def subtract_list_task(x: list[int | float]):
    if not isinstance(x, list):
        raise TypeError(f"Sub task expects a list, got {type(x).__name__}")

    if len(x) != 2:
        raise ValueError(f"Sub task expects 2 elements from chord, got {len(x)}")

    try:
        logger.info(f"Subtracting {x[0]} - {x[1]}")
        return x[0] - x[1]
    except Exception as e:
        logger.error(f"Error in subtraction: {e}")
        raise
