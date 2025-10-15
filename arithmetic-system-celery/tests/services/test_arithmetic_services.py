import pytest
from app.workers.add_service import add
from app.workers.sub_service import subtract
from app.workers.mul_service import multiply
from app.workers.div_service import divide
from app.workers.xsum_service import xsum

def test_add_task():
    assert add(5, 3) == 8
    assert add(-1, 1) == 0

def test_subtract_task():
    assert subtract(10, 4) == 6
    assert subtract(5, 10) == -5

def test_multiply_task():
    assert multiply(3, 4) == 12
    assert multiply(5, 0) == 0

def test_divide_task():
    assert divide(10, 2) == 5.0
    assert divide(5, 2) == 2.5

def test_divide_by_zero():
    with pytest.raises(ValueError, match="Cannot divide by zero"):
        divide(10, 0)

def test_xsum_task():
    assert xsum([1, 2, 3, 4]) == 10
    assert xsum([]) == 0