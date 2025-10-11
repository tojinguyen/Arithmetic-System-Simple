from celery import chain, group, Signature
from .expression_parser import ExpressionNode
from .combiner_service import combine_and_operate
import logging

logger = logging.getLogger(__name__)

class WorkflowBuilder:
    def __init__(self, task_map):
        self.task_map = task_map

    def build(self, node):
        workflow_signature = self._build_recursive(node)
        
        # Log cấu trúc workflow
        self._log_workflow_structure(workflow_signature)

        # Áp dụng và trả về AsyncResult
        return workflow_signature.apply_async()

    def _build_recursive(self, node) -> Signature | int | float:
        # Trường hợp cơ sở: node là một chiếc lá (một con số)
        if isinstance(node, (int, float)):
            return node

        # Trường hợp đệ quy: node là một phép toán
        if not isinstance(node, ExpressionNode):
            raise TypeError("Node không hợp lệ, phải là ExpressionNode hoặc số.")

        # Đệ quy xuống nhánh trái và phải
        left_operand = self._build_recursive(node.left)
        right_operand = self._build_recursive(node.right)

        op_task = self.task_map.get(node.operation)
        if not op_task:
            raise ValueError(f"Không tìm thấy task cho phép toán: {node.operation}")

        # --- Logic kết hợp kết quả từ các nhánh con ---

        is_left_task = isinstance(left_operand, Signature)
        is_right_task = isinstance(right_operand, Signature)

        # 1. Cả hai nhánh đều là số (ví dụ: 2 + 3) -> SIMPLE
        if not is_left_task and not is_right_task:
            return op_task.s(left_operand, right_operand)

        # 2. Nhánh trái là task, nhánh phải là số (ví dụ: (2+3) * 4) -> SEQUENTIAL
        elif is_left_task and not is_right_task:
            # chain(task, op.s(num)) -> op(task_result, num)
            return chain(left_operand, op_task.s(right_operand))
            
        # 3. Nhánh trái là số, nhánh phải là task (ví dụ: 5 - (1+2)) -> SEQUENTIAL
        elif not is_left_task and is_right_task:
            # Phép toán không giao hoán cần combiner để đảo ngược toán hạng
            # chain(task, combiner.s(op, num, is_left_fixed=True)) -> combiner(task_result, op, num) -> op(num, task_result)
            return chain(right_operand, combine_and_operate.s(node.operation, left_operand, is_left_fixed=True))

        # 4. Cả hai nhánh đều là task (ví dụ: (2+3) + (4*5)) -> PARALLEL / HYBRID
        else:
            # Chạy cả hai nhánh song song bằng `group`
            parallel_tasks = group(left_operand, right_operand)
            # Sau đó dùng `combine_and_operate` để kết hợp kết quả
            # chain(group, combiner.s(op)) -> combiner([left_res, right_res], op) -> op(left_res, right_res)
            return chain(parallel_tasks, combine_and_operate.s(node.operation))

    def _log_workflow_structure(self, sig, indent=0):
        """
        Hàm đệ quy để in ra cấu trúc của workflow một cách dễ đọc.
        """
        prefix = "  " * indent
        if indent == 0:
            logger.info("=" * 50)
            logger.info("CELERY WORKFLOW STRUCTURE")
            logger.info("-" * 50)

        if isinstance(sig, chain):
            logger.info(f"{prefix}CHAIN:")
            for task in sig.tasks:
                self._log_workflow_structure(task, indent + 1)
        elif isinstance(sig, group):
            logger.info(f"{prefix}GROUP (run in parallel):")
            for task in sig.tasks:
                self._log_workflow_structure(task, indent + 1)
        elif isinstance(sig, Signature):
            # Sử dụng repr để có một biểu diễn chi tiết về task
            logger.info(f"{prefix}TASK: {repr(sig)}")
        
        if indent == 0:
            logger.info("=" * 50)