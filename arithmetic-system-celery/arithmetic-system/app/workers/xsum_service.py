from ..celery import app
import logging

@app.task(name='xsum', queue='add_tasks')
def xsum(numbers):
    try:
        if not all(isinstance(i, (int, float)) for i in numbers):
            raise ValueError("All elements must be numbers.")
        result = sum(numbers)
        return result
    except Exception as e:
        logging.error(f"Error in summation: {e}")
        raise
