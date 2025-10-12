from celery import chain, group, Signature
from celery.result import EagerResult 
import uuid
from .combiner_service import combine_and_operate
from .expression_parser import ExpressionNode, ExpressionType
import logging

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
        
        left_op = self._build_recursive(node.left)
        right_op = self._build_recursive(node.right)
        
        is_left_task = isinstance(left_op, Signature)
        is_right_task = isinstance(right_op, Signature)
        
        op_name = node.operation.value
        
        if not is_left_task and not is_right_task:
            op_task = self.task_map[node.operation]
            return op_task.s(left_op, right_op)
        elif is_left_task and not is_right_task:
            return chain(
                left_op, 
                combine_and_operate.s(
                    operation_name=op_name, 
                    fixed_operand=right_op, 
                    is_left_fixed=False 
                )
            )
        elif not is_left_task and is_right_task:
            return chain(
                right_op, 
                combine_and_operate.s(
                    operation_name=op_name, 
                    fixed_operand=left_op, 
                    is_left_fixed=True
                )
            )
        else:
            parallel_tasks = group(left_op, right_op)
            return chain(parallel_tasks, combine_and_operate.s(operation_name=op_name))