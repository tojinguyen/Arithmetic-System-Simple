from celery import chain, group, Signature
from celery.result import EagerResult 
import uuid
from .combiner_service import combine_and_operate
from .expression_parser import ExpressionNode, ExpressionType, OperationEnum
import logging
from .xsum_service import xsum
from .xprod_service import xprod

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
        
        if node.operation.is_commutative:
            return self._build_optimized_workflow(node)
        else:
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
        
    def _collect_operands(self, node, operation: OperationEnum):
        operands = []
        if isinstance(node, ExpressionNode) and node.operation == operation:
            operands.extend(self._collect_operands(node.left, operation))
            operands.extend(self._collect_operands(node.right, operation))
        else:
            operands.append(node)
        return operands
    
    def _build_optimized_workflow(self, node: ExpressionNode) -> Signature | float:
        all_operands = self._collect_operands(node, node.operation)
        child_workflows = [self._build_recursive(op) for op in all_operands]
        
        tasks = [wf for wf in child_workflows if isinstance(wf, Signature)]
        constants = [wf for wf in child_workflows if not isinstance(wf, Signature)]

        if constants:
            if node.operation == OperationEnum.ADD:
                if len(constants) == 1:
                     pass 
                else:
                    constants_task = xsum.s(constants)
                    tasks.append(constants_task)

            elif node.operation == OperationEnum.MUL:
                if len(constants) == 1:
                    pass
                else:
                    constants_task = xprod.s(constants)
                    tasks.append(constants_task)
        
        if not tasks:
            if len(constants) == 1:
                return constants[0]
            return 1.0 if node.operation == OperationEnum.MUL else 0.0

        if len(tasks) == 1:
            if len(constants) == 1 and len(child_workflows) > 1:
                 return chain(tasks[0], combine_and_operate.s(
                    operation_name=node.operation.value,
                    fixed_operand=constants[0],
                    is_left_fixed=False 
                ))
            return tasks[0]

        parallel_group = group(tasks)
        logger.info(f"  - Created final parallel group: {parallel_group}")

        aggregator_task = xsum.s() if node.operation == OperationEnum.ADD else xprod.s()
        
        final_workflow = chain(parallel_group, aggregator_task)

        if len(constants) == 1 and len(child_workflows) > len(tasks):
             final_workflow |= combine_and_operate.s(
                    operation_name=node.operation.value,
                    fixed_operand=constants[0],
                    is_left_fixed=False
                )

        logger.info(f"  - Created final chain for group: {final_workflow}")
        return final_workflow
