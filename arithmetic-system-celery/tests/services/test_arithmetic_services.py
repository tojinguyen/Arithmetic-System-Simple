import pytest
from app.workers.add_service import add
from app.workers.sub_service import subtract
from app.workers.mul_service import multiply
from app.workers.div_service import divide
from app.workers.xsum_service import xsum


def test_add_task():
    assert add(5, 3) == 8
    assert add(-1, 1) == 0
    assert add(0, 0) == 0
    assert add(1.5, 2.5) == 4.0
    assert add(-3.5, -2.5) == -6.0


def test_subtract_task():
    assert subtract(10, 4) == 6
    assert subtract(5, 10) == -5
    assert subtract(0, 0) == 0
    assert subtract(-5, -5) == 0
    assert subtract(3.2, 1.2) == 2.0


def test_multiply_task():
    assert multiply(3, 4) == 12
    assert multiply(5, 0) == 0
    assert multiply(0, 0) == 0
    assert multiply(-3, 3) == -9
    assert multiply(1.5, 2) == 3.0
    assert multiply(-2.5, -4) == 10.0


def test_divide_task():
    assert divide(10, 2) == 5.0
    assert divide(5, 2) == 2.5
    assert divide(-9, 3) == -3.0
    assert divide(7.5, 2.5) == 3.0


def test_divide_by_zero():
    with pytest.raises(ValueError, match="Cannot divide by zero"):
        divide(10, 0)
    with pytest.raises(ValueError, match="Cannot divide by zero"):
        divide(0, 0)


def test_xsum_task():
    assert xsum([1, 2, 3, 4]) == 10
    assert xsum([]) == 0
    assert xsum([-1, -2, -3]) == -6
    assert xsum([1.5, 2.5, 3.0]) == 7.0
    assert xsum([0, 0, 0]) == 0
    assert xsum(range(1, 101)) == 5050


@pytest.mark.parametrize(
    "a, b, expected",
    [
        (1, 1, 2),
        (-2, 3, 1),
        (100, 200, 300),
        (1e6, 1e6, 2e6),
    ],
)
def test_add_parametrized(a, b, expected):
    assert add(a, b) == expected


@pytest.mark.parametrize(
    "numbers, expected",
    [
        ([10, 20, 30], 60),
        ([-5, 5, 10], 10),
        ([1.1, 2.2, 3.3], 6.6),
        ([1000, 2000, 3000], 6000),
    ],
)
def test_xsum_parametrized(numbers, expected):
    assert xsum(numbers) == pytest.approx(expected)
