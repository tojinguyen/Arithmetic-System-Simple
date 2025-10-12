import pytest
from app.services.add_service import add
from app.services.sub_service import subtract
from app.services.mul_service import multiply
from app.services.div_service import divide
from app.services.xsum_service import xsum

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