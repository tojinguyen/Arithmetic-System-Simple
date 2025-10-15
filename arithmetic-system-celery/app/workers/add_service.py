from ..celery import app
import logging

logger = logging.getLogger(__name__)


@app.task(name="add_task", queue="add_tasks")
def add_task(x: int | float, y: int | float, is_left_fixed: bool = False) -> float:
    try:
        logger.info(f"Adding {x} + {y} (Left fixed: {is_left_fixed}) Result: {x + y}")
        return x + y
    except Exception as exc:
        logger.error(f"Error in add_task: {exc}")
        raise
