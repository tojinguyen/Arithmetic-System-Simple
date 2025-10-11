from ..celery import app
import logging

@app.task(name='xsum', queue='add_tasks')
def xsum(numbers):
    try:
        logging.info(f"Calculating sum of {numbers}")
        if not all(isinstance(i, (int, float)) for i in numbers):
            raise ValueError("All elements must be numbers.")
        result = sum(numbers)
        logging.info(f"Result of sum: {result}")
        return result
    except Exception as e:
        logging.error(f"Error in summation: {e}")
        raise
