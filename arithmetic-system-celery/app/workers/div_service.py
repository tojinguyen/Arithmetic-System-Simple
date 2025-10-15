from ..celery import app
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@app.task(name="divide_task", queue="div_tasks")
def divide_task(x: int | float, y: int | float, is_left_fixed: bool = False) -> float:
    if not isinstance(x, (int, float)):
        raise TypeError(f"x must be int or float, got {type(x).__name__}")

    if y is not None and not isinstance(y, (int, float)):
        raise TypeError(f"y must be int or float, got {type(y).__name__}")

    dividend, divisor = (y, x) if is_left_fixed else (x, y)

    if divisor == 0:
        raise ZeroDivisionError(f"Cannot divide {dividend} by zero.")

    try:
        return dividend / divisor
    except Exception as e:
        logger.error(f"Error in division: {dividend} / {divisor}: {e}")
        raise
