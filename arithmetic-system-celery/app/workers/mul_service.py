from ..celery import app
import logging

logger = logging.getLogger(__name__)


@app.task(name="multiply_task", queue="mul_tasks")
def multiply_task(x: int | float, y: int | float, is_left_fixed: bool = False) -> float:
    try:
        logger.info(
            f"Multiplying {x} * {y} (Left fixed: {is_left_fixed}) Result: {x * y}"
        )
        return x * y
    except Exception as exc:
        logger.error(f"Error in multiply_task: {exc}")
        raise
