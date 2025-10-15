from ..celery import app
from math import prod

@app.task(name='xprod', queue='mul_tasks')
def xprod(numbers):
    try:
        if not all(isinstance(i, (int, float)) for i in numbers):
            raise ValueError("All elements must be numbers.")
        result = prod(numbers)
        return result
    except Exception as e:
        raise ValueError(f"Error in xprod task: {str(e)}")