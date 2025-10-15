from ..celery import app
import logging

logger = logging.getLogger(__name__)


@app.task(name="add", queue="add_tasks")
def add(x: int | float, y: int | float, is_left_fixed: bool = False) -> float:
    try:
        return x + y
    except Exception as exc:
        logger.error(f"Error in add_task: {exc}")
        raise
