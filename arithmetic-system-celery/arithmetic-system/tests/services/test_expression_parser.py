import pytest
from app.services.expression_parser import ExpressionParser, ExpressionNode, OperationEnum

@pytest.fixture
def parser():
    return ExpressionParser()

def test_parse_simple_addition(parser):
    """Expression: 2 + 3"""
    result = parser.parse("2 + 3")
    tree = result.expression_tree
    
    assert isinstance(tree, ExpressionNode)
    assert tree.operation == OperationEnum.ADD
    assert tree.left == 2.0
    assert tree.right == 3.0

def test_parse_with_precedence(parser):
    """Expression: 2 + 3 * 4"""
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
    """Expression: (2 + 3) * 4"""
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
    """Invalid expressions should raise ValueError"""
    with pytest.raises(ValueError, match="Invalid expression"):
        parser.parse("2 ++ 3")

    with pytest.raises(ValueError, match="invalid characters"):
        parser.parse("2 + a")