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
    result = parser.parse("2 + 3")
    tree = result.expression_tree
    assert isinstance(tree, ExpressionNode)
    assert tree.operation == OperationEnum.ADD
    assert tree.left == 2.0
    assert tree.right == 3.0


def test_parse_with_precedence(parser):
    result = parser.parse("2 + 3 * 4")
    tree = result.expression_tree
    assert tree.operation == OperationEnum.ADD
    assert tree.left == 2.0
    right_node = tree.right
    assert isinstance(right_node, ExpressionNode)
    assert right_node.operation == OperationEnum.MUL
    assert right_node.left == 3.0
    assert right_node.right == 4.0


def test_parse_with_parentheses(parser):
    result = parser.parse("(2 + 3) * 4")
    tree = result.expression_tree
    assert tree.operation == OperationEnum.MUL
    assert tree.right == 4.0
    left_node = tree.left
    assert isinstance(left_node, ExpressionNode)
    assert left_node.operation == OperationEnum.ADD
    assert left_node.left == 2.0
    assert left_node.right == 3.0


def test_parse_invalid_expression(parser):
    with pytest.raises(ValueError, match="Invalid expression"):
        parser.parse("2 ++ 3")
    with pytest.raises(ValueError, match="invalid characters"):
        parser.parse("2 + a")


def test_parse_subtraction(parser):
    result = parser.parse("10 - 4")
    tree = result.expression_tree
    assert tree.operation == OperationEnum.SUB
    assert tree.left == 10.0
    assert tree.right == 4.0


def test_parse_division(parser):
    result = parser.parse("8 / 2")
    tree = result.expression_tree
    assert tree.operation == OperationEnum.DIV
    assert tree.left == 8.0
    assert tree.right == 2.0


def test_parse_multiple_operations(parser):
    result = parser.parse("10 - 2 + 3")
    tree = result.expression_tree
    assert tree.operation == OperationEnum.ADD
    assert isinstance(tree.left, ExpressionNode)
    assert tree.left.operation == OperationEnum.SUB
    assert tree.left.left == 10.0
    assert tree.left.right == 2.0
    assert tree.right == 3.0


def test_parse_nested_parentheses(parser):
    result = parser.parse("((2 + 3) * (4 - 1)) / 5")
    tree = result.expression_tree
    assert tree.operation == OperationEnum.DIV
    assert tree.right == 5.0
    left_mul = tree.left
    assert left_mul.operation == OperationEnum.MUL
    left_add = left_mul.left
    right_sub = left_mul.right
    assert left_add.operation == OperationEnum.ADD
    assert left_add.left == 2.0
    assert left_add.right == 3.0
    assert right_sub.operation == OperationEnum.SUB
    assert right_sub.left == 4.0
    assert right_sub.right == 1.0


def test_parse_with_spaces(parser):
    result = parser.parse("  7   *   ( 2 + 5 ) ")
    tree = result.expression_tree
    assert tree.operation == OperationEnum.MUL
    assert tree.left == 7.0
    assert isinstance(tree.right, ExpressionNode)
    assert tree.right.operation == OperationEnum.ADD
    assert tree.right.left == 2.0
    assert tree.right.right == 5.0


def test_parse_decimal_numbers(parser):
    result = parser.parse("3.5 + 2.25 * 4")
    tree = result.expression_tree
    assert tree.operation == OperationEnum.ADD
    assert tree.left == 3.5
    right = tree.right
    assert right.operation == OperationEnum.MUL
    assert right.left == 2.25
    assert right.right == 4.0


def test_parse_negative_numbers(parser):
    result = parser.parse("-3 + 2")
    tree = result.expression_tree
    assert tree.operation == OperationEnum.ADD
    assert tree.left == -3.0
    assert tree.right == 2.0


def test_parse_complex_expression(parser):
    result = parser.parse("2 + 3 * (4 - 2) / (1 + 1)")
    tree = result.expression_tree
    assert tree.operation == OperationEnum.ADD
    assert tree.left == 2.0
    right = tree.right
    assert right.operation == OperationEnum.DIV
    assert right.right.operation == OperationEnum.ADD
    assert right.left.operation == OperationEnum.MUL
    assert right.left.left == 3.0
    assert right.left.right.operation == OperationEnum.SUB
