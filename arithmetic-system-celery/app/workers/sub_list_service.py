from ..celery import app
import logging

logger = logging.getLogger(__name__)


@app.task(name="sub", queue="sub_tasks")
def sub(x: list[int | float]):
    if not isinstance(x, list):
        raise TypeError(f"Sub task expects a list, got {type(x).__name__}")

    if len(x) != 2:
        raise ValueError(f"Sub task expects 2 elements from chord, got {len(x)}")

    try:
        return x[0] - x[1]
    except Exception as e:
        logger.error(f"Error in subtraction: {e}")
        raise
