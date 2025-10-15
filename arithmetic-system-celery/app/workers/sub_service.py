from ..celery import app
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.task(name='subtract', queue='sub_tasks')
def subtract(x, y=None , is_left_fixed=False):
    try:
        if isinstance(x, list):
            if len(x) != 2:
                raise ValueError(f"Subtract task expects 2 elements from chord, got {len(x)}")
            return x[0] - x[1]
        if is_left_fixed:
            return y - x
        else:
            return x - y
    except Exception as exc:
        logger.error(f"Error in subtract_task: {exc}")
        raise

