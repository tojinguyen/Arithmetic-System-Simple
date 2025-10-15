from ..celery import app
import logging

logger = logging.getLogger(__name__)


@app.task(name="subtract_task", queue="sub_tasks")
def subtract_task(x: float, y: float = None, is_left_fixed: bool = False) -> float:
    if not isinstance(x, (int, float)):
        raise TypeError(f"x must be int or float, got {type(x).__name__}")

    if not isinstance(y, (int, float)):
        raise TypeError(f"y must be int or float, got {type(y).__name__}")

    minuend, subtrahend = (y, x) if is_left_fixed else (x, y)

    try:
        return minuend - subtrahend
    except Exception as exc:
        logger.error(f"Error in subtract task: {minuend} - {subtrahend}: {exc}")
        raise
