import pytest
from app.workers.add_service import add_task
from app.workers.sub_service import subtract_task
from app.workers.mul_service import multiply_task
from app.workers.div_service import divide_task
from app.workers.xsum_service import xsum_task


def test_add_task():
    assert add_task(5, 3) == 8
    assert add_task(-1, 1) == 0
    assert add_task(0, 0) == 0
    assert add_task(1.5, 2.5) == 4.0
    assert add_task(-3.5, -2.5) == -6.0


def test_subtract_task():
    assert subtract_task(10, 4) == 6
    assert subtract_task(5, 10) == -5
    assert subtract_task(0, 0) == 0
    assert subtract_task(-5, -5) == 0
    assert subtract_task(3.2, 1.2) == 2.0


def test_multiply_task():
    assert multiply_task(3, 4) == 12
    assert multiply_task(5, 0) == 0
    assert multiply_task(0, 0) == 0
    assert multiply_task(-3, 3) == -9
    assert multiply_task(1.5, 2) == 3.0
    assert multiply_task(-2.5, -4) == 10.0


def test_divide_task():
    assert divide_task(10, 2) == 5.0
    assert divide_task(5, 2) == 2.5
    assert divide_task(-9, 3) == -3.0
    assert divide_task(7.5, 2.5) == 3.0


def test_divide_by_zero():
    with pytest.raises(ZeroDivisionError, match="Cannot divide .* by zero"):
        divide_task(10, 0)
    with pytest.raises(ZeroDivisionError, match="Cannot divide .* by zero"):
        divide_task(0, 0)


def test_xsum_task():
    assert xsum_task([1, 2, 3, 4]) == 10
    assert xsum_task([]) == 0
    assert xsum_task([-1, -2, -3]) == -6
    assert xsum_task([1.5, 2.5, 3.0]) == 7.0
    assert xsum_task([0, 0, 0]) == 0
    assert xsum_task(list(range(1, 101))) == 5050


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
    assert add_task(a, b) == expected


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
    assert xsum_task(numbers) == pytest.approx(expected)
