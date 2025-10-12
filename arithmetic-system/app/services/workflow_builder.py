from celery import chain, group, Signature
from celery.result import EagerResult 
from ..celery import app
import uuid

from .expression_parser import ExpressionNode, OperationEnum, ExpressionType
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
        
        left_operand = self._build_recursive(node.left)
        right_operand = self._build_recursive(node.right)
        
        if isinstance(left_operand, float) and isinstance(right_operand, float):
            task_function = self.task_map[node.operation]
            return task_function.s(left_operand, right_operand)
        
        raise NotImplementedError("This simple builder only handles single operations or numbers.")