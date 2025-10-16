import pytest
from unittest.mock import Mock
from celery import Signature
from celery.result import EagerResult

from app.services.workflow_builder import WorkflowBuilder
from app.services.expression_parser import ExpressionNode, OperationEnum


@pytest.fixture
def mock_tasks():
    """Create mock Celery tasks"""

    def create_signature_mock(task_name):
        """Helper to create proper signature mock"""

        def signature_creator(*args, **kwargs):
            sig = Mock(spec=Signature)
            sig.task = task_name
            sig.args = args
            sig.kwargs = kwargs
            sig.options = {}
            sig.apply_async = Mock(return_value=Mock(spec=EagerResult))
            sig.__or__ = Mock(return_value=Mock(spec=Signature))
            return sig

        return signature_creator

    add_task = Mock()
    add_task.name = "add_task"
    add_task.s = Mock(side_effect=create_signature_mock("app.workers.add_task"))

    subtract_task = Mock()
    subtract_task.name = "subtract_task"
    subtract_task.s = Mock(
        side_effect=create_signature_mock("app.workers.subtract_task")
    )

    multiply_task = Mock()
    multiply_task.name = "multiply_task"
    multiply_task.s = Mock(
        side_effect=create_signature_mock("app.workers.multiply_task")
    )

    divide_task = Mock()
    divide_task.name = "divide_task"
    divide_task.s = Mock(side_effect=create_signature_mock("app.workers.divide_task"))

    subtract_list_task = Mock()
    subtract_list_task.name = "subtract_list_task"
    subtract_list_task.s = Mock(
        side_effect=create_signature_mock("app.workers.subtract_list_task")
    )

    divide_list_task = Mock()
    divide_list_task.name = "divide_list_task"
    divide_list_task.s = Mock(
        side_effect=create_signature_mock("app.workers.divide_list_task")
    )

    return {
        "add": add_task,
        "subtract": subtract_task,
        "multiply": multiply_task,
        "divide": divide_task,
        "subtract_list": subtract_list_task,
        "divide_list": divide_list_task,
    }


@pytest.fixture
def task_map(mock_tasks):
    """Create task map for WorkflowBuilder"""
    return {
        OperationEnum.ADD: mock_tasks["add"],
        OperationEnum.SUB: mock_tasks["subtract"],
        OperationEnum.MUL: mock_tasks["multiply"],
        OperationEnum.DIV: mock_tasks["divide"],
    }


@pytest.fixture
def task_chord_map(mock_tasks):
    """Create chord task map for WorkflowBuilder"""
    return {
        OperationEnum.SUB: mock_tasks["subtract_list"],
        OperationEnum.DIV: mock_tasks["divide_list"],
    }


@pytest.fixture
def workflow_builder(task_map, task_chord_map):
    """Create WorkflowBuilder instance"""
    return WorkflowBuilder(task_map, task_chord_map)


class TestWorkflowBuilderIntegration:
    """Integration tests for WorkflowBuilder with real expression trees"""

    def test_constants(self, workflow_builder):
        """Test workflow building with constants"""
        # Positive constant
        result, workflow_str = workflow_builder.build(42)
        assert isinstance(result, EagerResult)
        assert result.result == 42
        assert workflow_str == "constant(42)"

        # Zero constant
        result, workflow_str = workflow_builder.build(0)
        assert isinstance(result, EagerResult)
        assert result.result == 0
        assert workflow_str == "constant(0)"

        # Negative constant
        result, workflow_str = workflow_builder.build(-5)
        assert isinstance(result, EagerResult)
        assert result.result == -5
        assert workflow_str == "constant(-5)"

        # Float constant
        result, workflow_str = workflow_builder.build(3.14)
        assert isinstance(result, EagerResult)
        assert result.result == 3.14
        assert workflow_str == "constant(3.14)"

    def test_simple_operations(self, workflow_builder):
        """Test simple binary operations"""
        # Addition
        node = ExpressionNode(operation=OperationEnum.ADD, left=1, right=2)
        result, workflow_str = workflow_builder.build(node)
        assert result is not None
        assert workflow_str == "add_task(1, 2)"

        # Subtraction
        node = ExpressionNode(operation=OperationEnum.SUB, left=5, right=3)
        result, workflow_str = workflow_builder.build(node)
        assert result is not None
        assert workflow_str == "subtract_task(5, 3)"

        # Multiplication
        node = ExpressionNode(operation=OperationEnum.MUL, left=4, right=6)
        result, workflow_str = workflow_builder.build(node)
        assert result is not None
        assert workflow_str == "multiply_task(4, 6)"

        # Division
        node = ExpressionNode(operation=OperationEnum.DIV, left=8, right=2)
        result, workflow_str = workflow_builder.build(node)
        assert result is not None
        assert workflow_str == "divide_task(8, 2)"

    def test_commutative_operations_flattening(self, workflow_builder):
        """Test flattening of commutative operations"""
        # Three additions: (1 + 2) + 3
        inner_node = ExpressionNode(operation=OperationEnum.ADD, left=1, right=2)
        outer_node = ExpressionNode(
            operation=OperationEnum.ADD, left=inner_node, right=3
        )
        result, workflow_str = workflow_builder.build(outer_node)
        assert result is not None
        assert "xsum_task([1, 2, 3])" in workflow_str

        # Three multiplications: (2 * 3) * 4
        inner_node = ExpressionNode(operation=OperationEnum.MUL, left=2, right=3)
        outer_node = ExpressionNode(
            operation=OperationEnum.MUL, left=inner_node, right=4
        )
        result, workflow_str = workflow_builder.build(outer_node)
        assert result is not None
        assert "xprod_task([2, 3, 4])" in workflow_str

        # Complex flattening: ((1 + 2) + (3 + 4))
        left_inner = ExpressionNode(operation=OperationEnum.ADD, left=1, right=2)
        right_inner = ExpressionNode(operation=OperationEnum.ADD, left=3, right=4)
        outer_node = ExpressionNode(
            operation=OperationEnum.ADD, left=left_inner, right=right_inner
        )
        result, workflow_str = workflow_builder.build(outer_node)
        assert result is not None
        assert workflow_str == "xsum_task([1, 2, 3, 4])"

        # Deep nesting: (((((1 + 2) + 3) + 4) + 5) + 6)
        current = ExpressionNode(operation=OperationEnum.ADD, left=1, right=2)
        for i in range(3, 7):
            current = ExpressionNode(operation=OperationEnum.ADD, left=current, right=i)
        result, workflow_str = workflow_builder.build(current)
        assert result is not None
        assert workflow_str == "xsum_task([1, 2, 3, 4, 5, 6])"

    def test_mixed_operations(self, workflow_builder, mock_tasks):
        """Test mixed operations creating chains and chords"""
        # Chain: (1 + 2) - 3
        inner_node = ExpressionNode(operation=OperationEnum.ADD, left=1, right=2)
        outer_node = ExpressionNode(
            operation=OperationEnum.SUB, left=inner_node, right=3
        )
        result, workflow_str = workflow_builder.build(outer_node)
        assert result is not None
        mock_tasks["add"].s.assert_called_with(1, 2)
        mock_tasks["subtract"].s.assert_called_with(y=3)

        # Chord: (2 * 6) / (1 + 2)
        left_expr = ExpressionNode(operation=OperationEnum.MUL, left=2, right=6)
        right_expr = ExpressionNode(operation=OperationEnum.ADD, left=1, right=2)
        outer_node = ExpressionNode(
            operation=OperationEnum.DIV, left=left_expr, right=right_expr
        )
        result, workflow_str = workflow_builder.build(outer_node)
        assert result is not None
        assert (
            workflow_str
            == "chord([multiply_task(2, 6), add_task(1, 2)], divide_list_task)"
        )

    def test_complex_expressions(self, workflow_builder):
        """Test complex nested expressions"""
        # Complex parallel branches: (1 + 2 + 3) - (4 * 5 * 6)
        left_inner = ExpressionNode(operation=OperationEnum.ADD, left=1, right=2)
        left_outer = ExpressionNode(
            operation=OperationEnum.ADD, left=left_inner, right=3
        )

        right_inner = ExpressionNode(operation=OperationEnum.MUL, left=4, right=5)
        right_outer = ExpressionNode(
            operation=OperationEnum.MUL, left=right_inner, right=6
        )

        final_expr = ExpressionNode(
            operation=OperationEnum.SUB, left=left_outer, right=right_outer
        )
        result, workflow_str = workflow_builder.build(final_expr)
        assert result is not None
        assert (
            workflow_str
            == "chord([xsum_task([1, 2, 3]), xprod_task([4, 5, 6])], subtract_list_task)"
        )

        # Mixed deeply nested: ((1 + 2 + 3) * (4 + 5 + 6))
        left_level1 = ExpressionNode(operation=OperationEnum.ADD, left=1, right=2)
        left_final = ExpressionNode(
            operation=OperationEnum.ADD, left=left_level1, right=3
        )

        right_level1 = ExpressionNode(operation=OperationEnum.ADD, left=4, right=5)
        right_final = ExpressionNode(
            operation=OperationEnum.ADD, left=right_level1, right=6
        )

        final_expr = ExpressionNode(
            operation=OperationEnum.MUL, left=left_final, right=right_final
        )
        result, workflow_str = workflow_builder.build(final_expr)
        assert result is not None
        assert (
            workflow_str
            == "chord([xsum_task([1, 2, 3]), xsum_task([4, 5, 6])], xprod_task)"
        )

    def test_error_handling(self, workflow_builder):
        """Test error handling for invalid inputs"""
        # Invalid input type
        with pytest.raises(TypeError, match="Invalid node type"):
            workflow_builder.build("invalid")

        # Invalid node type in recursive method
        with pytest.raises(TypeError, match="Invalid node type"):
            workflow_builder._build_recursive("not a node")
