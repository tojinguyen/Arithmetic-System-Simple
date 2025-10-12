from celery import chain, group, Signature
from celery.result import EagerResult 
from ..celery import app
import uuid

from .expression_parser import ExpressionNode, OperationEnum, ExpressionType
from .combiner_service import combine_and_operate
from .xsum_service import xsum
from .xprod_service import xprod
import logging
from math import prod

logger = logging.getLogger(__name__)

class WorkflowBuilder:
    def __init__(self, task_map):
        self.task_map = task_map
        self.metadata = {"chains": 0, "groups": 0}

    def build(self, node):
        self.metadata = {"chains": 0, "groups": 0}
        logger.info("="*20 + " STARTING WORKFLOW BUILD " + "="*20)
        
        workflow_or_result = self._build_recursive(node)
        
        if isinstance(workflow_or_result, (int, float)):
            logger.info(f"===> Simple expression pre-calculated to: {workflow_or_result}")
            task_id = str(uuid.uuid4())
            async_result = EagerResult(task_id, workflow_or_result, 'SUCCESS')
            final_workflow_signature_for_logging = f"PRE_CALCULATED_VALUE({workflow_or_result})"
            expr_type = ExpressionType.SIMPLE.value

        else:
            final_workflow_signature_for_logging = str(workflow_or_result)
            async_result = workflow_or_result.apply_async()
            
            is_hybrid = self.metadata["chains"] > 0 and self.metadata["groups"] > 0
            is_parallel = self.metadata["groups"] > 0 and self.metadata["chains"] == 0
            is_sequential = self.metadata["chains"] > 0 and self.metadata["groups"] == 0
            
            if is_hybrid:
                expr_type = ExpressionType.HYBRID.value
            elif is_parallel:
                expr_type = ExpressionType.PARALLEL.value
            elif is_sequential:
                expr_type = ExpressionType.SEQUENTIAL.value
            else: 
                expr_type = ExpressionType.SEQUENTIAL.value if self.metadata["chains"] > 0 else ExpressionType.SIMPLE.value

        self.metadata["type"] = expr_type
        logger.info(f"===> FINAL WORKFLOW: {final_workflow_signature_for_logging}")
        logger.info(f"===> METADATA: {self.metadata}")
        logger.info("="*20 + "  ENDING WORKFLOW BUILD  " + "="*20)

        return async_result, self.metadata

    def _collect_operands(self, node, operation: OperationEnum):
        operands = []
        if isinstance(node, ExpressionNode) and node.operation == operation:
            operands.extend(self._collect_operands(node.left, operation))
            operands.extend(self._collect_operands(node.right, operation))
        else:
            operands.append(node)
        return operands

    def _build_recursive(self, node) -> Signature | float:
        if isinstance(node, (int, float)):
            return float(node)

        if not isinstance(node, ExpressionNode):
            raise TypeError("Invalid node type")

        if node.operation.is_commutative:
            logger.info(f"\n--- Optimizing Commutative Node: [{node.operation.value.upper()}] ---")
            
            all_operands = self._collect_operands(node, node.operation)
            
            child_workflows = [self._build_recursive(op) for op in all_operands]
            
            tasks = [wf for wf in child_workflows if isinstance(wf, Signature)]
            constants = [wf for wf in child_workflows if not isinstance(wf, Signature)]
            logger.info(f"  - Collected: {len(tasks)} sub-tasks, {len(constants)} constants ({constants})")

            initial_value = 0.0
            if constants:
                if node.operation == OperationEnum.ADD:
                    initial_value = sum(constants)
                elif node.operation == OperationEnum.MUL:
                    initial_value = prod(constants)
            
            if not tasks:
                return initial_value

            if len(tasks) == 1 and not constants:
                return tasks[0]
            
            if len(tasks) == 1 and constants:
                 self.metadata["chains"] += 1
                 return chain(tasks[0], combine_and_operate.s(
                    operation_name=node.operation.value,
                    fixed_operand=initial_value,
                    is_left_fixed=False
                ))

            self.metadata["groups"] += 1
            parallel_group = group(tasks)
            logger.info(f"  - Created parallel group: {parallel_group}")

            aggregator_task = xsum.s() if node.operation == OperationEnum.ADD else xprod.s()
            
            self.metadata["chains"] += 1
            final_workflow = chain(parallel_group, aggregator_task)

            if constants: 
                self.metadata["chains"] += 1
                final_workflow |= combine_and_operate.s(
                    operation_name=node.operation.value,
                    fixed_operand=initial_value,
                    is_left_fixed=False 
                )
            
            logger.info(f"  - Created final chain for group: {final_workflow}")
            return final_workflow

        else:
            logger.info(f"\n--- Handling Sequential Node: [{node.operation.value.upper()}] ---")
            left_op = self._build_recursive(node.left)
            right_op = self._build_recursive(node.right)
            logger.info(f"  - Left operand: {left_op}")
            logger.info(f"  - Right operand: {right_op}")

            op_task = self.task_map[node.operation]
            is_left_task = isinstance(left_op, Signature)
            is_right_task = isinstance(right_op, Signature)
            
            if not is_left_task and not is_right_task:
                 task_func = op_task.undecorated_func
                 return task_func(left_op, right_op)

            self.metadata["chains"] += 1
            if is_left_task and not is_right_task:
                return chain(left_op, combine_and_operate.s(operation_name=node.operation.value, fixed_operand=right_op, is_left_fixed=False))
            
            if not is_left_task and is_right_task:
                return chain(right_op, combine_and_operate.s(operation_name=node.operation.value, fixed_operand=left_op, is_left_fixed=True))
            
            self.metadata["groups"] += 1
            parallel_tasks = group(left_op, right_op)
            return chain(parallel_tasks, combine_and_operate.s(node.operation.value))