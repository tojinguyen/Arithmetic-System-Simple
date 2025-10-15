from ..celery import app
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.task(name='divide', queue='div_tasks')
def divide(x, y=None, is_left_fixed=False):
    try:
        if isinstance(x, list):
            if len(x) != 2:
                raise ValueError(f"Divide task expects 2 elements from chord, got {len(x)}")
            if x[1] == 0:
                raise ValueError("Cannot divide by zero.")
            return x[0] / x[1]

        if is_left_fixed:
            if x == 0:
                raise ValueError("Cannot divide by zero.")
            return y / x
        else:
            if y == 0:
                raise ValueError("Cannot divide by zero.")
            return x / y
    except Exception as e:
        logger.error(f"Error in division: {e}")
        raise
