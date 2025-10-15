from celery import chain, group, Signature, chord
from celery.result import EagerResult
import uuid
from .expression_parser import ExpressionNode, OperationEnum
import logging
from ..workers.xsum_service import xsum
from ..workers.xprod_service import xprod

logger = logging.getLogger(__name__)


class WorkflowBuilder:
    def __init__(self, task_map):
        self.task_map = task_map

    def build(self, node):
        workflow_or_result = self._build_recursive(node)

        if isinstance(workflow_or_result, (int, float)):
            task_id = str(uuid.uuid4())
            async_result = EagerResult(task_id, workflow_or_result, 'SUCCESS')
        elif isinstance(workflow_or_result, Signature):
            async_result = workflow_or_result.apply_async()
        else:
            raise TypeError(f"Build process returned an unexpected type: {type(workflow_or_result)}")

        return async_result

    def _build_recursive(self, node) -> Signature | float:
        if isinstance(node, (int, float)):
            return float(node)

        if not isinstance(node, ExpressionNode):
            raise TypeError(f"Invalid node type: {type(node)}")

        is_left_constant = isinstance(node.left, (int, float))
        is_right_constant = isinstance(node.right, (int, float))
        if is_left_constant and is_right_constant:
            op_task = self.task_map[node.operation]
            return op_task.s(node.left, node.right)

        if node.operation.is_commutative and not is_left_constant and not is_right_constant:
            return self._build_flat_workflow(node)
        else:
            left_workflow = self._build_recursive(node.left)
            right_workflow = self._build_recursive(node.right)

            is_left_task = isinstance(left_workflow, Signature)
            is_right_task = isinstance(right_workflow, Signature)

            op_task = self.task_map[node.operation]

            if is_left_task and not is_right_task:
                return chain(left_workflow, op_task.s(y=right_workflow, is_left_fixed=False))
            elif not is_left_task and is_right_task:
                return chain(right_workflow, op_task.s(y=left_workflow, is_left_fixed=True))
            else:
                parallel_tasks = group(left_workflow, right_workflow)
                return chord(parallel_tasks, op_task.s())

    def _collect_operands(self, node, operation: OperationEnum):
        operands = []
        if isinstance(node, ExpressionNode) and node.operation == operation:
            operands.extend(self._collect_operands(node.left, operation))
            operands.extend(self._collect_operands(node.right, operation))
        else:
            operands.append(node)
        return operands

    def _build_flat_workflow(self, node: ExpressionNode) -> Signature | float:
        op_task = self.task_map[node.operation]

        all_operands = self._collect_operands(node, node.operation)
        child_workflows = [self._build_recursive(op) for op in all_operands]

        tasks = [wf for wf in child_workflows if isinstance(wf, Signature)]
        constants = [wf for wf in child_workflows if not isinstance(wf, Signature)]

        if constants:
            if node.operation == OperationEnum.ADD:
                if len(constants) > 1:
                    constants_task = xsum.s(constants)
                    tasks.append(constants_task)

            elif node.operation == OperationEnum.MUL:
                if len(constants) > 1:
                    constants_task = xprod.s(constants)
                    tasks.append(constants_task)

        if not tasks:
            if len(constants) == 1:
                return constants[0]
            return 1.0 if node.operation == OperationEnum.MUL else 0.0

        if len(tasks) == 1:
            if len(constants) == 1 and len(child_workflows) > 1:
                return chain(tasks[0], op_task.s(y=constants[0], is_left_fixed=False))
            return tasks[0]

        parallel_group = group(tasks)
        logger.info(f"  - Created final parallel group: {parallel_group}")

        aggregator_task = xsum.s() if node.operation == OperationEnum.ADD else xprod.s()

        final_workflow = chord(header=group(tasks), body=aggregator_task)

        if len(constants) == 1 and len(child_workflows) > len(tasks):
            return chain(tasks[0], op_task.s(y=constants[0], is_left_fixed=False))

        logger.info(f"  - Created final chain for group: {final_workflow}")
        return final_workflow
