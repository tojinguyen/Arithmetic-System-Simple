# File: arithmetic-system/app/services/workflow_builder.py

from celery import chain, group, Signature
# ### THÊM IMPORT NÀY ###
from celery.result import EagerResult 
from ..celery import app # Import a celery app instance to get task id
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

    # ### HÀM BUILD ĐÃ ĐƯỢC SỬA LẠI ###
    def build(self, node):
        self.metadata = {"chains": 0, "groups": 0}
        logger.info("="*20 + " STARTING WORKFLOW BUILD " + "="*20)
        
        # _build_recursive có thể trả về một số float hoặc một Signature
        workflow_or_result = self._build_recursive(node)
        
        # === XỬ LÝ LỖI Ở ĐÂY ===
        # Trường hợp 1: Kết quả là một con số (biểu thức đơn giản đã được tính toán)
        if isinstance(workflow_or_result, (int, float)):
            logger.info(f"===> Simple expression pre-calculated to: {workflow_or_result}")
            # Tạo một AsyncResult "giả" đã hoàn thành để Orchestrator có thể .get()
            # Điều này giữ cho luồng logic trong orchestrator không đổi.
            task_id = str(uuid.uuid4())
            async_result = EagerResult(task_id, workflow_or_result, 'SUCCESS')
            final_workflow_signature_for_logging = f"PRE_CALCULATED_VALUE({workflow_or_result})"
            expr_type = ExpressionType.SIMPLE.value

        # Trường hợp 2: Kết quả là một Signature, cần được thực thi
        else:
            final_workflow_signature_for_logging = str(workflow_or_result)
            async_result = workflow_or_result.apply_async()
            
            # Xác định expression type dựa trên metadata
            is_hybrid = self.metadata["chains"] > 0 and self.metadata["groups"] > 0
            is_parallel = self.metadata["groups"] > 0 and self.metadata["chains"] == 0
            is_sequential = self.metadata["chains"] > 0 and self.metadata["groups"] == 0
            
            if is_hybrid:
                expr_type = ExpressionType.HYBRID.value
            elif is_parallel:
                expr_type = ExpressionType.PARALLEL.value
            elif is_sequential:
                expr_type = ExpressionType.SEQUENTIAL.value
            else: # Single task, e.g., (10-2)*3
                # Nếu có chain, nó là sequential. Nếu không, là simple.
                expr_type = ExpressionType.SEQUENTIAL.value if self.metadata["chains"] > 0 else ExpressionType.SIMPLE.value

        self.metadata["type"] = expr_type
        logger.info(f"===> FINAL WORKFLOW: {final_workflow_signature_for_logging}")
        logger.info(f"===> METADATA: {self.metadata}")
        logger.info("="*20 + "  ENDING WORKFLOW BUILD  " + "="*20)

        # Trả về AsyncResult (thật hoặc giả) và metadata
        return async_result, self.metadata

    # Các hàm _collect_operands và _build_recursive giữ nguyên như phiên bản trước.
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

        # OPTIMIZATION: Handle commutative operations by flattening the tree
        if node.operation.is_commutative:
            logger.info(f"\n--- Optimizing Commutative Node: [{node.operation.value.upper()}] ---")
            
            all_operands = self._collect_operands(node, node.operation)
            
            # Recursively build workflows for each operand
            child_workflows = [self._build_recursive(op) for op in all_operands]
            
            tasks = [wf for wf in child_workflows if isinstance(wf, Signature)]
            constants = [wf for wf in child_workflows if not isinstance(wf, Signature)]
            logger.info(f"  - Collected: {len(tasks)} sub-tasks, {len(constants)} constants ({constants})")

            # Pre-calculate constants
            initial_value = 0.0
            if constants:
                if node.operation == OperationEnum.ADD:
                    initial_value = sum(constants)
                elif node.operation == OperationEnum.MUL:
                    initial_value = prod(constants)
            
            if not tasks:
                return initial_value

            # If there's only one task and no constants to combine, just return it
            # Sửa đổi: nếu có hằng số, vẫn phải tạo chain
            if len(tasks) == 1 and not constants:
                return tasks[0]
            
            if len(tasks) == 1 and constants:
                 self.metadata["chains"] += 1
                 return chain(tasks[0], combine_and_operate.s(
                    operation_name=node.operation.value,
                    fixed_operand=initial_value,
                    is_left_fixed=False
                ))

            # Create a parallel group for all sub-tasks
            self.metadata["groups"] += 1
            parallel_group = group(tasks)
            logger.info(f"  - Created parallel group: {parallel_group}")

            # Select aggregator task (xsum or xprod)
            aggregator_task = xsum.s() if node.operation == OperationEnum.ADD else xprod.s()
            
            # Chain the group result into the aggregator
            self.metadata["chains"] += 1
            final_workflow = chain(parallel_group, aggregator_task)

            # If there were constants, add a final step to combine them
            if constants: # Check against original list, not just `initial_value` in case of mul with 1 or add with 0
                self.metadata["chains"] += 1
                final_workflow |= combine_and_operate.s(
                    operation_name=node.operation.value,
                    fixed_operand=initial_value,
                    is_left_fixed=False 
                )
            
            logger.info(f"  - Created final chain for group: {final_workflow}")
            return final_workflow

        # DEFAULT: Handle non-commutative operations sequentially
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

            # Nếu chỉ một trong hai là task, hoặc cả hai là task
            self.metadata["chains"] += 1
            if is_left_task and not is_right_task:
                return chain(left_op, combine_and_operate.s(operation_name=node.operation.value, fixed_operand=right_op, is_left_fixed=False))
            
            if not is_left_task and is_right_task:
                return chain(right_op, combine_and_operate.s(operation_name=node.operation.value, fixed_operand=left_op, is_left_fixed=True))
            
            # Both are tasks
            self.metadata["groups"] += 1
            parallel_tasks = group(left_op, right_op)
            return chain(parallel_tasks, combine_and_operate.s(node.operation.value))