from ..celery import app
import logging
from math import prod

@app.task(name='xprod', queue='mul_tasks')
def xprod(numbers):
    try:
        logging.info(f"Calculating product of {numbers}")
        if not all(isinstance(i, (int, float)) for i in numbers):
            raise ValueError("All elements must be numbers.")
        result = prod(numbers)
        logging.info(f"Result of product: {result}")
        return result
    except Exception as e:
        logging.error(f"Error in product calculation: {e}")
        raise