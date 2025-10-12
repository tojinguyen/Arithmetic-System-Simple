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
        """Xử lý việc xây dựng workflow song song tối đa cho các phép toán giao hoán."""
        logger.info(f"\n--- Optimizing Commutative Node: [{node.operation.value.upper()}] ---")
        
        all_operands = self._collect_operands(node, node.operation)
        child_workflows = [self._build_recursive(op) for op in all_operands]
        
        tasks = [wf for wf in child_workflows if isinstance(wf, Signature)]
        constants = [wf for wf in child_workflows if not isinstance(wf, Signature)]
        logger.info(f"  - Collected: {len(tasks)} sub-tasks, {len(constants)} constants ({constants})")

        # 1. Nếu có hằng số, tạo một task để tính toán chúng
        if constants:
            if node.operation == OperationEnum.ADD:
                # Nếu chỉ có một hằng số, không cần xsum, chỉ cần giá trị của nó
                if len(constants) == 1:
                     # Để xử lý sau, ta tạm coi nó là một "task" đã có kết quả
                     pass 
                else:
                    constants_task = xsum.s(constants)
                    tasks.append(constants_task) # Thêm task này vào danh sách task chung
                    logger.info(f"  - Created constants task: {constants_task}")

            elif node.operation == OperationEnum.MUL:
                if len(constants) == 1:
                    pass
                else:
                    constants_task = xprod.s(constants)
                    tasks.append(constants_task)
                    logger.info(f"  - Created constants task: {constants_task}")
        
        # 2. Xử lý các trường hợp sau khi đã gộp task hằng số
        
        # Nếu không có task nào cả (chỉ có hằng số và đã được tính)
        # Điều này chỉ xảy ra nếu tất cả toán hạng là hằng số
        if not tasks:
            # Nếu chỉ có 1 hằng số, trả về nó
            if len(constants) == 1:
                return constants[0]
            # Nếu không thì không có gì để làm (trường hợp rỗng, nên trả về giá trị đơn vị)
            return 1.0 if node.operation == OperationEnum.MUL else 0.0

        # Nếu chỉ còn lại một task (có thể là task gốc hoặc task hằng số)
        if len(tasks) == 1:
            # Nếu có 1 hằng số và 1 task
            if len(constants) == 1 and len(child_workflows) > 1:
                 return chain(tasks[0], combine_and_operate.s(
                    operation_name=node.operation.value,
                    fixed_operand=constants[0],
                    is_left_fixed=False 
                ))
            return tasks[0]

        # 3. Nếu có nhiều task (bao gồm cả task hằng số), tạo group và aggregator
        parallel_group = group(tasks)
        logger.info(f"  - Created final parallel group: {parallel_group}")

        aggregator_task = xsum.s() if node.operation == OperationEnum.ADD else xprod.s()
        
        final_workflow = chain(parallel_group, aggregator_task)

        # Nếu có một hằng số duy nhất, kết hợp nó vào cuối cùng
        if len(constants) == 1 and len(child_workflows) > len(tasks):
             final_workflow |= combine_and_operate.s(
                    operation_name=node.operation.value,
                    fixed_operand=constants[0],
                    is_left_fixed=False
                )

        logger.info(f"  - Created final chain for group: {final_workflow}")
        return final_workflow
