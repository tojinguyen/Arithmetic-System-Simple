import pytest
from celery import Signature, group
from unittest.mock import MagicMock

from app.services.workflow_builder import WorkflowBuilder
from app.services.expression_parser import ExpressionNode, OperationEnum


@pytest.fixture
def mock_task_map():
    return {
        OperationEnum.ADD: MagicMock(name="add_task", return_value=Signature("add")),
        OperationEnum.SUB: MagicMock(name="sub_task", return_value=Signature("sub")),
        OperationEnum.MUL: MagicMock(name="mul_task", return_value=Signature("mul")),
        OperationEnum.DIV: MagicMock(name="div_task", return_value=Signature("div")),
    }


@pytest.fixture
def builder(mock_task_map):
    from app.workers import xsum_service, xprod_service

    xsum_service.xsum = MagicMock(name="xsum_task", return_value=Signature("xsum"))
    xprod_service.xprod = MagicMock(name="xprod_task", return_value=Signature("xprod"))

    return WorkflowBuilder(mock_task_map)


def test_build_single_constant(builder):
    node = 5.0
    result = builder._build_recursive(node)
    assert result == 5.0


def test_build_simple_operation_with_constants(builder, mock_task_map):
    node = ExpressionNode(operation=OperationEnum.ADD, left=2.0, right=3.0)
    add_signature = Signature("add", args=(2.0, 3.0))
    mock_task_map[OperationEnum.ADD].s.return_value = add_signature

    workflow = builder._build_recursive(node)

    assert isinstance(workflow, Signature)
    assert workflow.task == "add"
    assert workflow.args == (2.0, 3.0)
    mock_task_map[OperationEnum.ADD].s.assert_called_once_with(2.0, 3.0)


def test_build_simple_chain_left_heavy(builder, mock_task_map):
    mul_node = ExpressionNode(operation=OperationEnum.MUL, left=2.0, right=3.0)
    root_node = ExpressionNode(operation=OperationEnum.ADD, left=mul_node, right=4.0)

    mul_sig = Signature("mul", args=(2.0, 3.0))
    add_sig = Signature("add", kwargs={"y": 4.0, "is_left_fixed": False})

    mock_task_map[OperationEnum.MUL].s.return_value = mul_sig
    mock_task_map[OperationEnum.ADD].s.return_value = add_sig

    workflow = builder._build_recursive(root_node)

    assert isinstance(workflow, Signature)
    assert workflow.task == "celery.chain"

    assert len(workflow.tasks) == 2
    assert workflow.tasks[0].task == "mul"
    assert workflow.tasks[0].args == (2.0, 3.0)

    assert workflow.tasks[1].task == "add"
    assert workflow.tasks[1].kwargs == {"y": 4.0, "is_left_fixed": False}


def test_build_simple_chain_right_heavy(builder, mock_task_map):
    mul_node = ExpressionNode(operation=OperationEnum.MUL, left=2.0, right=3.0)
    root_node = ExpressionNode(operation=OperationEnum.SUB, left=10.0, right=mul_node)

    mul_sig = Signature("mul", args=(2.0, 3.0))
    sub_sig = Signature("sub", kwargs={"y": 10.0, "is_left_fixed": True})

    mock_task_map[OperationEnum.MUL].s.return_value = mul_sig
    mock_task_map[OperationEnum.SUB].s.return_value = sub_sig

    workflow = builder._build_recursive(root_node)

    assert isinstance(workflow, Signature)
    assert workflow.task == "celery.chain"
    assert len(workflow.tasks) == 2
    assert workflow.tasks[0].task == "mul"
    assert workflow.tasks[1].task == "sub"
    assert workflow.tasks[1].kwargs == {"y": 10.0, "is_left_fixed": True}


def test_build_parallel_chord(builder, mock_task_map):
    left_node = ExpressionNode(operation=OperationEnum.MUL, left=2.0, right=3.0)
    right_node = ExpressionNode(operation=OperationEnum.MUL, left=4.0, right=5.0)
    root_node = ExpressionNode(
        operation=OperationEnum.ADD, left=left_node, right=right_node
    )

    mul_sig1 = Signature("mul", args=(2.0, 3.0))
    mul_sig2 = Signature("mul", args=(4.0, 5.0))
    add_sig_callback = Signature("add")

    mock_task_map[OperationEnum.MUL].s.side_effect = [mul_sig1, mul_sig2]
    mock_task_map[OperationEnum.ADD].s.return_value = add_sig_callback

    workflow = builder._build_recursive(root_node)

    assert isinstance(workflow, Signature)
    assert workflow.task == "celery.chord"

    header_group = workflow.kwargs["header"]
    body_callback = workflow.kwargs["body"]

    assert isinstance(header_group, group)
    assert len(header_group.tasks) == 2
    assert header_group.tasks[0].task == "mul"
    assert header_group.tasks[0].args == (2.0, 3.0)
    assert header_group.tasks[1].task == "mul"
    assert header_group.tasks[1].args == (4.0, 5.0)

    assert body_callback.task == "add"


def test_build_flat_commutative_workflow(builder, mock_task_map):
    node1 = ExpressionNode(OperationEnum.MUL, 2.0, 3.0)
    node2 = ExpressionNode(OperationEnum.MUL, 5.0, 6.0)
    root_node = ExpressionNode(
        OperationEnum.ADD,
        ExpressionNode(
            OperationEnum.ADD, ExpressionNode(OperationEnum.ADD, 1.0, node1), 4.0
        ),
        node2,
    )

    mul_sig1 = Signature("mul", args=(2.0, 3.0))
    mul_sig2 = Signature("mul", args=(5.0, 6.0))
    xsum_constants_sig = Signature("xsum", args=([1.0, 4.0],))
    xsum_aggregator_sig = Signature("xsum")

    mock_task_map[OperationEnum.MUL].s.side_effect = [mul_sig1, mul_sig2]
    mock_task_map[OperationEnum.ADD].s.return_value = xsum_aggregator_sig

    from app.workers import xsum_service

    xsum_service.xsum.s.side_effect = [xsum_constants_sig, xsum_aggregator_sig]

    workflow = builder._build_recursive(root_node)

    assert isinstance(workflow, Signature)
    assert workflow.task == "celery.chord"

    header_group = workflow.kwargs["header"]
    body_callback = workflow.kwargs["body"]

    assert isinstance(header_group, group)
    assert len(header_group.tasks) == 3

    task_names = {t.task for t in header_group.tasks}
    assert task_names == {"mul", "xsum"}

    assert body_callback.task == "xsum"
