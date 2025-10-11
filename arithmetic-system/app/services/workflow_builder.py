from celery import chain, group, Signature
from .expression_parser import ExpressionNode
from .combiner_service import combine_and_operate

class WorkflowBuilder:
    def __init__(self, task_map):
        self.task_map = task_map

    def build(self, node):
        workflow_signature = self._build_recursive(node)
        return workflow_signature.apply_async()

    def _build_recursive(self, node):
        # Trường hợp cơ sở: node là một chiếc lá (một con số)
        if isinstance(node, (int, float)):
            return node

        # Trường hợp đệ quy: node là một phép toán
        if not isinstance(node, ExpressionNode):
            raise TypeError("Node không hợp lệ")

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
            return chain(left_operand, op_task.s(right_operand))
            
        # 3. Nhánh trái là số, nhánh phải là task (ví dụ: 5 - (1+2)) -> SEQUENTIAL
        elif not is_left_task and is_right_task:
            # Đối với phép toán không giao hoán, cần một task đặc biệt để đảo ngược toán hạng
            # Đây là lúc chúng ta cần `combine_and_operate`
            return chain(right_operand, combine_and_operate.s(node.operation, left_operand, is_left_fixed=True))

        # 4. Cả hai nhánh đều là task (ví dụ: (2+3) + (4*5)) -> PARALLEL / HYBRID
        else:
            # Chạy cả hai nhánh song song bằng `group`
            # Sau đó dùng `combine_and_operate` để kết hợp kết quả
            parallel_tasks = group(left_operand, right_operand)
            return chain(parallel_tasks, combine_and_operate.s(node.operation))