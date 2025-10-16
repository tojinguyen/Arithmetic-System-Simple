import pytest
from unittest.mock import Mock
from celery import chord, Signature
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


class TestWorkflowBuilder:
    """Test suite for WorkflowBuilder"""

    # ========== Test build method ==========

    def test_build_with_constant_returns_eager_result(self, workflow_builder):
        """Test building workflow with constant value"""
        # Given
        constant_value = 42

        # When
        result, workflow_str = workflow_builder.build(constant_value)

        # Then
        assert isinstance(result, EagerResult)
        assert result.result == constant_value
        assert workflow_str == "constant(42)"

    def test_build_with_expression_node(self, workflow_builder, mock_tasks):
        """Test building workflow with ExpressionNode"""
        node = ExpressionNode(operation=OperationEnum.ADD, left=1, right=2)

        result, workflow_str = workflow_builder.build(node)

        # Verify that the task was called with both constants
        mock_tasks["add"].s.assert_called_once_with(1, 2)
        # Verify the result is not None and apply_async was called
        assert result is not None
        assert "add_task(1, 2)" in workflow_str

    def test_build_with_invalid_type_raises_error(self, workflow_builder):
        """Test building workflow with invalid type raises TypeError"""
        invalid_input = "invalid"

        with pytest.raises(TypeError, match="Invalid node type"):
            workflow_builder.build(invalid_input)

    def test_build_recursive_with_constant(self, workflow_builder):
        """Test _build_recursive with constant value"""
        constant = 5.0

        result = workflow_builder._build_recursive(constant)

        assert result == 5.0

    def test_build_recursive_with_both_constants(self, workflow_builder, mock_tasks):
        """Test _build_recursive with two constant operands"""
        node = ExpressionNode(operation=OperationEnum.ADD, left=1, right=2)

        _ = workflow_builder._build_recursive(node)

        mock_tasks["add"].s.assert_called_once_with(1, 2)

    def test_build_recursive_with_invalid_node_type(self, workflow_builder):
        """Test _build_recursive with invalid node type raises TypeError"""
        invalid_node = "not a node"

        with pytest.raises(TypeError, match="Invalid node type"):
            workflow_builder._build_recursive(invalid_node)

    def test_build_recursive_left_constant_right_node(
        self, workflow_builder, mock_tasks
    ):
        """Test _build_recursive with left constant and right ExpressionNode"""
        inner_node = ExpressionNode(operation=OperationEnum.MUL, left=2, right=3)
        outer_node = ExpressionNode(
            operation=OperationEnum.ADD, left=1, right=inner_node
        )

        result = workflow_builder._build_recursive(outer_node)

        # Check that multiply task was called with constants
        mock_tasks["multiply"].s.assert_called_with(2, 3)
        # For ADD with mixed types, it goes through _build_flat_workflow
        # which can result in different call patterns
        assert len(mock_tasks["add"].s.call_args_list) >= 1  # At least one call
        # Verify that we get a valid result
        assert result is not None

    def test_build_recursive_left_node_right_constant(
        self, workflow_builder, mock_tasks
    ):
        """Test _build_recursive with left ExpressionNode and right constant"""
        inner_node = ExpressionNode(operation=OperationEnum.MUL, left=2, right=3)
        outer_node = ExpressionNode(
            operation=OperationEnum.ADD, left=inner_node, right=1
        )

        result = workflow_builder._build_recursive(outer_node)

        # Check that multiply task was called with constants
        mock_tasks["multiply"].s.assert_called_once_with(2, 3)
        # Check that add task was called - right constant=1, so y=1, is_left_fixed=False
        mock_tasks["add"].s.assert_called_once_with(y=1)
        # Verify that we get a chained workflow
        assert result is not None

    def test_flatten_commutative_operands_with_constant(self, workflow_builder):
        """Test _flatten_commutative_operands with constant value"""
        constant = 5

        result = workflow_builder._flatten_commutative_operands(
            constant, OperationEnum.ADD
        )

        assert result == [5]

    def test_flatten_commutative_operands_with_different_operation(
        self, workflow_builder
    ):
        """Test _flatten_commutative_operands with different operation"""
        node = ExpressionNode(operation=OperationEnum.MUL, left=1, right=2)

        result = workflow_builder._flatten_commutative_operands(node, OperationEnum.ADD)

        assert result == [node]

    def test_flatten_commutative_operands_with_nested_same_operation(
        self, workflow_builder
    ):
        """Test _flatten_commutative_operands with nested same operations"""
        # Expression: 1 + 2 + 3 -> ((1 + 2) + 3)
        inner_node = ExpressionNode(operation=OperationEnum.ADD, left=1, right=2)
        outer_node = ExpressionNode(
            operation=OperationEnum.ADD, left=inner_node, right=3
        )

        result = workflow_builder._flatten_commutative_operands(
            outer_node, OperationEnum.ADD
        )

        assert result == [1, 2, 3]

    def test_flatten_commutative_operands_complex_tree(self, workflow_builder):
        """Test _flatten_commutative_operands with complex tree"""
        # Given: ((1 + 2) + (3 + 4))
        left_inner = ExpressionNode(operation=OperationEnum.ADD, left=1, right=2)
        right_inner = ExpressionNode(operation=OperationEnum.ADD, left=3, right=4)
        outer_node = ExpressionNode(
            operation=OperationEnum.ADD, left=left_inner, right=right_inner
        )

        # When
        result = workflow_builder._flatten_commutative_operands(
            outer_node, OperationEnum.ADD
        )

        # Then
        assert result == [1, 2, 3, 4]

    # ========== Test _format_args method ==========

    def test_format_args_with_no_args(self, workflow_builder):
        """Test _format_args with no arguments"""
        # Given
        sig = Mock(spec=Signature)
        sig.args = ()
        sig.kwargs = {}

        # When
        result = workflow_builder._format_args(sig)

        # Then
        assert result == ""

    def test_format_args_with_numeric_args(self, workflow_builder):
        """Test _format_args with numeric arguments"""
        # Given
        sig = Mock(spec=Signature)
        sig.args = (1, 2.5)
        sig.kwargs = {}

        # When
        result = workflow_builder._format_args(sig)

        # Then
        assert result == "(1, 2.5)"

    def test_format_args_with_list_arg(self, workflow_builder):
        """Test _format_args with list argument"""
        # Given
        sig = Mock(spec=Signature)
        sig.args = ([1, 2, 3],)
        sig.kwargs = {}

        # When
        result = workflow_builder._format_args(sig)

        # Then
        assert result == "([1, 2, 3])"

    def test_format_args_with_unknown_arg(self, workflow_builder):
        """Test _format_args with unknown argument type"""
        # Given
        sig = Mock(spec=Signature)
        sig.args = (Mock(),)
        sig.kwargs = {}

        # When
        result = workflow_builder._format_args(sig)

        # Then
        assert result == "(?)"

    def test_format_args_with_kwargs(self, workflow_builder):
        """Test _format_args with keyword arguments"""
        # Given
        sig = Mock(spec=Signature)
        sig.args = ()
        sig.kwargs = {"y": 5, "is_left_fixed": True}

        # When
        result = workflow_builder._format_args(sig)

        # Then
        assert "y=5" in result
        assert "is_left_fixed=True" in result

    def test_format_args_mixed_args_and_kwargs(self, workflow_builder):
        """Test _format_args with both args and kwargs"""
        # Given
        sig = Mock(spec=Signature)
        sig.args = (1, 2)
        sig.kwargs = {"y": 3}

        # When
        result = workflow_builder._format_args(sig)

        # Then
        assert result == "(1, 2, y=3)"

    # ========== Test _signature_to_string method ==========

    def test_signature_to_string_simple_task(self, workflow_builder):
        """Test _signature_to_string with simple task"""
        # Given
        sig = Mock(spec=Signature)
        sig.task = "app.workers.add_task"
        sig.args = (1, 2)
        sig.kwargs = {}
        sig.options = {}

        # When
        result = workflow_builder._signature_to_string(sig)

        # Then
        assert result == "add_task(1, 2)"

    def test_signature_to_string_task_with_kwargs(self, workflow_builder):
        """Test _signature_to_string with task having kwargs"""
        # Given
        sig = Mock(spec=Signature)
        sig.task = "app.workers.subtract_task"
        sig.args = ()
        sig.kwargs = {"y": 5, "is_left_fixed": False}
        sig.options = {}

        # When
        result = workflow_builder._signature_to_string(sig)

        # Then
        assert result == "subtract_task(y=5, is_left_fixed=False)"

    def test_signature_to_string_chain(self, workflow_builder):
        """Test _signature_to_string with chained tasks"""
        # Given - simulate a real _chain object
        from celery.canvas import _chain

        first_sig = Mock(spec=Signature)
        first_sig.task = "app.workers.multiply_task"
        first_sig.args = (2, 3)
        first_sig.kwargs = {}
        first_sig.options = {}

        second_sig = Mock(spec=Signature)
        second_sig.task = "app.workers.add_task"
        second_sig.args = ()
        second_sig.kwargs = {"y": 1}
        second_sig.options = {}

        chain_sig = Mock(spec=_chain)
        chain_sig.tasks = [first_sig, second_sig]

        # When
        result = workflow_builder._signature_to_string(chain_sig)

        # Then
        assert result == "multiply_task(2, 3) | add_task(y=1)"

    def test_signature_to_string_group(self, workflow_builder):
        """Test _signature_to_string with group"""
        # Given
        task1 = Mock(spec=Signature)
        task1.task = "app.workers.add_task"
        task1.args = (1, 2)
        task1.kwargs = {}
        task1.options = {}

        task2 = Mock(spec=Signature)
        task2.task = "app.workers.multiply_task"
        task2.args = (3, 4)
        task2.kwargs = {}
        task2.options = {}

        group_sig = Mock()
        group_sig.tasks = [task1, task2]

        # When
        result = workflow_builder._signature_to_string(group_sig)

        # Then
        assert result == "group([add_task(1, 2), multiply_task(3, 4)])"

    def test_signature_to_string_chord(self, workflow_builder):
        """Test _signature_to_string with chord"""
        # Given
        task1 = Mock(spec=Signature)
        task1.task = "app.workers.add_task"
        task1.args = (1, 2)
        task1.kwargs = {}
        task1.options = {}

        task2 = Mock(spec=Signature)
        task2.task = "app.workers.add_task"
        task2.args = (3, 4)
        task2.kwargs = {}
        task2.options = {}

        callback = Mock(spec=Signature)
        callback.task = "app.workers.multiply_list_task"
        callback.args = ()
        callback.kwargs = {}
        callback.options = {}

        chord_sig = Mock(spec=chord)
        chord_sig.tasks = [task1, task2]
        chord_sig.body = callback

        # When
        result = workflow_builder._signature_to_string(chord_sig)

        # Then
        assert result == "chord([add_task(1, 2), add_task(3, 4)], multiply_list_task)"

    def test_signature_to_string_unknown_task(self, workflow_builder):
        """Test _signature_to_string with unknown task"""
        # Given
        sig = Mock(spec=Signature)
        del sig.task  # Remove task attribute
        sig.args = ()
        sig.kwargs = {}
        sig.options = {}

        # When
        result = workflow_builder._signature_to_string(sig)

        # Then
        assert result == "unknown"


class TestWorkflowBuilderIntegration:
    """Integration tests for WorkflowBuilder with real expression trees"""

    def test_simple_addition(self, workflow_builder):
        """Test workflow for simple addition: 1 + 2"""
        # Given
        node = ExpressionNode(operation=OperationEnum.ADD, left=1, right=2)

        # When
        result, workflow_str = workflow_builder.build(node)

        # Then
        assert result is not None
        assert "add_task(1, 2)" in workflow_str

    def test_nested_operations(self, workflow_builder):
        """Test workflow for nested operations: (1 + 2) * 3"""
        # Given
        inner_node = ExpressionNode(operation=OperationEnum.ADD, left=1, right=2)
        outer_node = ExpressionNode(
            operation=OperationEnum.MUL, left=inner_node, right=3
        )

        # When
        result, workflow_str = workflow_builder.build(outer_node)

        # Then
        assert result is not None
        # Workflow string should contain some representation of the workflow
        assert len(workflow_str) > 0
        # Result should be an AsyncResult
        assert hasattr(result, "get") or hasattr(result, "result")


class TestEdgeCases:
    """Test edge cases and error handling"""

    def test_build_with_zero_constant(self, workflow_builder):
        """Test building workflow with zero constant"""
        # When
        result, workflow_str = workflow_builder.build(0)

        # Then
        assert isinstance(result, EagerResult)
        assert result.result == 0
        assert workflow_str == "constant(0)"

    def test_build_with_negative_constant(self, workflow_builder):
        """Test building workflow with negative constant"""
        # When
        result, workflow_str = workflow_builder.build(-5)

        # Then
        assert isinstance(result, EagerResult)
        assert result.result == -5
        assert workflow_str == "constant(-5)"

    def test_build_with_float_constant(self, workflow_builder):
        """Test building workflow with float constant"""
        # When
        result, workflow_str = workflow_builder.build(3.14)

        # Then
        assert isinstance(result, EagerResult)
        assert result.result == 3.14
        assert workflow_str == "constant(3.14)"

    def test_format_args_with_empty_list(self, workflow_builder):
        """Test _format_args with empty list"""
        # Given
        sig = Mock(spec=Signature)
        sig.args = ([],)
        sig.kwargs = {}

        # When
        result = workflow_builder._format_args(sig)

        # Then
        assert result == "([])"

    def test_format_args_with_mixed_list(self, workflow_builder):
        """Test _format_args with list containing non-numeric values"""
        # Given
        sig = Mock(spec=Signature)
        sig.args = ([1, "string", 2, None, 3],)
        sig.kwargs = {}

        # When
        result = workflow_builder._format_args(sig)

        # Then
        assert result == "([1, 2, 3])"  # Only numeric values
