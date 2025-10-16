from celery import group, Signature, chord
from celery.result import EagerResult, AsyncResult
import uuid
from .expression_parser import ExpressionNode, OperationEnum
import logging
from app.workers import xsum_task, xprod_task
from typing import Callable
from celery.canvas import _chain

logger = logging.getLogger(__name__)


class WorkflowBuilder:
    def __init__(
        self,
        task_map: dict[OperationEnum, Callable[..., int | float]],
        task_chord_map: dict[OperationEnum, Callable[..., int | float]] = None,
    ):
        self.task_map = task_map
        self.task_chord_map = task_chord_map

    def build(self, node) -> tuple[AsyncResult, str]:
        workflow_or_result = self._build_recursive(node)
        workflow_string = ""

        if isinstance(workflow_or_result, (int, float)):
            task_id = str(uuid.uuid4())
            async_result = EagerResult(task_id, workflow_or_result, "SUCCESS")
            workflow_string = f"constant({workflow_or_result})"
        elif isinstance(workflow_or_result, Signature):
            async_result = workflow_or_result.apply_async()
            workflow_string = self._signature_to_string(workflow_or_result)
        else:
            raise TypeError(
                f"Build process returned an unexpected type: {type(workflow_or_result)}"
            )

        return async_result, workflow_string

    def _build_recursive(self, node) -> Signature | float | int:
        if isinstance(node, (int, float)):
            return node

        if not isinstance(node, ExpressionNode):
            raise TypeError(f"Invalid node type: {type(node)}")

        is_left_constant = isinstance(node.left, (int, float))
        is_right_constant = isinstance(node.right, (int, float))

        # Both are constants
        if is_left_constant and is_right_constant:
            op_task = self.task_map[node.operation]
            logger.info(f"Building constant workflow for operation: {node.operation}")
            return op_task.s(node.left, node.right)

        # Both are same operation and commutative
        if node.operation.is_commutative and (
            not is_left_constant or not is_right_constant
        ):
            return self._build_flat_workflow(node)

        left_workflow = self._build_recursive(node.left)
        right_workflow = self._build_recursive(node.right)

        op_task = self.task_map[node.operation]

        # Left is constant, Right is OperationNode
        if not is_left_constant and is_right_constant:
            return left_workflow | op_task.s(y=right_workflow)

        # Left is OperationNode, Right is constant
        if is_left_constant and not is_right_constant:
            return right_workflow | op_task.s(y=left_workflow, is_left_fixed=True)

        # Both are OperationNodes
        op_chord_task = self.task_chord_map.get(node.operation)
        parallel_tasks = group(left_workflow, right_workflow)
        return chord(parallel_tasks, op_chord_task.s())

    def _build_flat_workflow(self, node: ExpressionNode) -> Signature | float:
        op_task = self.task_map[node.operation]
        aggregator_task = (
            xsum_task if node.operation == OperationEnum.ADD else xprod_task
        )

        flatten_commutative_nodes = self._flatten_commutative_operands(
            node, node.operation
        )

        child_workflows = [
            self._build_recursive(sub_node) for sub_node in flatten_commutative_nodes
        ]

        tasks = [
            workflow for workflow in child_workflows if isinstance(workflow, Signature)
        ]
        constants = [
            workflow
            for workflow in child_workflows
            if not isinstance(workflow, Signature)
        ]

        identity = 0.0 if node.operation == OperationEnum.ADD else 1.0
        num_tasks = len(tasks)
        num_constants = len(constants)

        # Case: No tasks (only constants)
        if not tasks:
            if num_constants == 0:
                return identity
            if num_constants == 1:
                return constants[0]
            if num_constants == 2:
                return op_task.s(constants[0], constants[1])
            return aggregator_task.s(constants)

        # Case: Single task
        if num_tasks == 1:
            if not constants:
                return tasks[0]
            if num_constants == 1:
                return tasks[0] | op_task.s(y=constants[0])
            # num_constants > 1
            tasks.append(aggregator_task.s(constants))
            return chord(header=group(tasks), body=aggregator_task.s())

        # Case: Multiple tasks
        if num_constants == 1:
            return chord(header=group(tasks), body=aggregator_task.s()) | op_task.s(
                y=constants[0]
            )
        if num_constants > 1:
            tasks.append(aggregator_task.s(constants))
            return chord(header=group(tasks), body=aggregator_task.s())

        return chord(header=group(tasks), body=aggregator_task.s())

    def _flatten_commutative_operands(
        self, node, operation: OperationEnum
    ) -> list[ExpressionNode | float | int]:
        sub_commutative_expression: list[ExpressionNode | float | int] = []

        if node is None:
            return sub_commutative_expression

        if not isinstance(node, ExpressionNode) or node.operation != operation:
            sub_commutative_expression.append(node)
            return sub_commutative_expression

        sub_commutative_expression.extend(
            self._flatten_commutative_operands(node.left, operation)
        )
        sub_commutative_expression.extend(
            self._flatten_commutative_operands(node.right, operation)
        )

        return sub_commutative_expression

    def _signature_to_string(self, sig: Signature) -> str:
        # Chore
        if isinstance(sig, chord):
            header_tasks = [self._signature_to_string(task) for task in sig.tasks]
            body_str = self._signature_to_string(sig.body)
            return f"chord([{', '.join(header_tasks)}], {body_str})"

        # Chain
        if isinstance(sig, _chain):
            task_strings = [self._signature_to_string(task) for task in sig.tasks]
            return " | ".join(task_strings)

        # Group
        if hasattr(sig, "tasks"):
            task_strings = [self._signature_to_string(task) for task in sig.tasks]
            return f"group([{', '.join(task_strings)}])"

        # Single Task
        task_name = sig.task.split(".")[-1] if hasattr(sig, "task") else "unknown"
        args_str = self._format_args(sig)
        task_with_args = f"{task_name}{args_str}" if args_str else task_name

        return task_with_args

    def _format_args(self, sig: Signature) -> str:
        args = []
        kwargs = {}

        if hasattr(sig, "args") and sig.args:
            args = list(sig.args)

        if hasattr(sig, "kwargs") and sig.kwargs:
            kwargs = dict(sig.kwargs)

        parts = []

        for arg in args:
            if isinstance(arg, (int, float)):
                parts.append(str(arg))
            elif isinstance(arg, list):
                list_str = ", ".join(str(x) for x in arg if isinstance(x, (int, float)))
                parts.append(f"[{list_str}]")
            else:
                parts.append("?")

        for key, value in kwargs.items():
            if isinstance(value, (int, float)):
                parts.append(f"{key}={value}")
            elif key in ["is_left_fixed"]:
                parts.append(f"{key}={value}")
            else:
                parts.append(f"{key}=?")

        return f"({', '.join(parts)})" if parts else ""
