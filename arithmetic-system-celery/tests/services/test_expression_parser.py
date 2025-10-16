import pytest
from app.services.expression_parser import (
    ExpressionParser,
    ExpressionNode,
    OperationEnum,
)


@pytest.fixture
def parser():
    return ExpressionParser()


def test_parse_simple_addition(parser):
    tree = parser.parse("2 + 3")
    assert isinstance(tree, ExpressionNode)
    assert tree.operation == OperationEnum.ADD
    assert tree.left == 2
    assert tree.right == 3


def test_parse_with_precedence(parser):
    tree = parser.parse("2 + 3 * 4")
    assert tree.operation == OperationEnum.ADD
    assert tree.left == 2
    right_node = tree.right
    assert isinstance(right_node, ExpressionNode)
    assert right_node.operation == OperationEnum.MUL
    assert right_node.left == 3
    assert right_node.right == 4


def test_parse_with_parentheses(parser):
    tree = parser.parse("(2 + 3) * 4")
    assert tree.operation == OperationEnum.MUL
    assert tree.right == 4
    left_node = tree.left
    assert isinstance(left_node, ExpressionNode)
    assert left_node.operation == OperationEnum.ADD
    assert left_node.left == 2
    assert left_node.right == 3


def test_parse_invalid_expression(parser):
    with pytest.raises(Exception):  # Could be UnsupportedUnaryOperatorError
        parser.parse("2 ++ 3")
    with pytest.raises(Exception):  # Could be ExpressionSyntaxError
        parser.parse("2 + a")


def test_parse_subtraction(parser):
    tree = parser.parse("10 - 4")
    assert tree.operation == OperationEnum.SUB
    assert tree.left == 10
    assert tree.right == 4


def test_parse_division(parser):
    tree = parser.parse("8 / 2")
    assert tree.operation == OperationEnum.DIV
    assert tree.left == 8
    assert tree.right == 2


def test_parse_multiple_operations(parser):
    tree = parser.parse("10 - 2 + 3")
    assert tree.operation == OperationEnum.ADD
    assert isinstance(tree.left, ExpressionNode)
    assert tree.left.operation == OperationEnum.SUB
    assert tree.left.left == 10
    assert tree.left.right == 2
    assert tree.right == 3


def test_parse_nested_parentheses(parser):
    tree = parser.parse("((2 + 3) * (4 - 1)) / 5")
    assert tree.operation == OperationEnum.DIV
    assert tree.right == 5
    left_mul = tree.left
    assert left_mul.operation == OperationEnum.MUL
    left_add = left_mul.left
    right_sub = left_mul.right
    assert left_add.operation == OperationEnum.ADD
    assert left_add.left == 2
    assert left_add.right == 3
    assert right_sub.operation == OperationEnum.SUB
    assert right_sub.left == 4
    assert right_sub.right == 1


def test_parse_with_spaces(parser):
    tree = parser.parse("  7   *   ( 2 + 5 ) ")
    assert tree.operation == OperationEnum.MUL
    assert tree.left == 7
    assert isinstance(tree.right, ExpressionNode)
    assert tree.right.operation == OperationEnum.ADD
    assert tree.right.left == 2
    assert tree.right.right == 5


def test_parse_decimal_numbers(parser):
    tree = parser.parse("3.5 + 2.25 * 4")
    assert tree.operation == OperationEnum.ADD
    assert tree.left == 3.5
    right = tree.right
    assert right.operation == OperationEnum.MUL
    assert right.left == 2.25
    assert right.right == 4


def test_parse_negative_numbers(parser):
    tree = parser.parse("-3 + 2")
    assert tree.operation == OperationEnum.ADD
    assert tree.left == -3
    assert tree.right == 2


def test_parse_complex_expression(parser):
    tree = parser.parse("2 + 3 * (4 - 2) / (1 + 1)")
    assert tree.operation == OperationEnum.ADD
    assert tree.left == 2
    right = tree.right
    assert right.operation == OperationEnum.DIV
    assert right.right.operation == OperationEnum.ADD
    assert right.left.operation == OperationEnum.MUL
    assert right.left.left == 3
    assert right.left.right.operation == OperationEnum.SUB
