from ..celery import app
import logging

logger = logging.getLogger(__name__)


@app.task(name="multiply", queue="mul_tasks")
def multiply(x: int | float, y: int | float, is_left_fixed: bool = False) -> float:
    try:
        return x * y
    except Exception as exc:
        logger.error(f"Error in multiply_task: {exc}")
        raise
