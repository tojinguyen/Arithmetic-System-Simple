from celery import chain, group, Signature
from .expression_parser import ExpressionNode
from .combiner_service import combine_and_operate
import logging

logger = logging.getLogger(__name__)

class WorkflowBuilder:
    def __init__(self, task_map):
        self.task_map = task_map

    def build(self, node):
        logger.info("="*20 + " BẮT ĐẦU XÂY DỰNG WORKFLOW " + "="*20) # DÒNG THÊM MỚI
        workflow_signature = self._build_recursive(node)
        logger.info(f"===> WORKFLOW CUỐI CÙNG ĐƯỢC TẠO: {workflow_signature}") # DÒNG THÊM MỚI
        logger.info("="*20 + " KẾT THÚC XÂY DỰNG WORKFLOW " + "="*20) # DÒNG THÊM MỚI

        # Áp dụng và trả về AsyncResult
        return workflow_signature.apply_async()

    def _build_recursive(self, node) -> Signature | int | float:
        # Trường hợp cơ sở: node là một chiếc lá (một con số)
        if isinstance(node, (int, float)):
            return node

        # Trường hợp đệ quy: node là một phép toán
        if not isinstance(node, ExpressionNode):
            raise TypeError("Node không hợp lệ, phải là ExpressionNode hoặc số.")

        logger.info(f"\n--- Đang xử lý node: [{node.operation.upper()}] ở level {node.level} ---") # DÒNG THÊM MỚI

        # Đệ quy xuống nhánh trái và phải
        left_operand = self._build_recursive(node.left)
        right_operand = self._build_recursive(node.right)

        logger.info(f"  - Nhánh trái trả về: {left_operand} (type: {type(left_operand).__name__})") # DÒNG THÊM MỚI
        logger.info(f"  - Nhánh phải trả về: {right_operand} (type: {type(right_operand).__name__})") # DÒNG THÊM MỚI

        op_task = self.task_map.get(node.operation)
        if not op_task:
            raise ValueError(f"Không tìm thấy task cho phép toán: {node.operation}")

        # --- Logic kết hợp kết quả từ các nhánh con ---

        is_left_task = isinstance(left_operand, Signature)
        is_right_task = isinstance(right_operand, Signature)

        # 1. Cả hai nhánh đều là số (ví dụ: 2 + 3) -> SIMPLE
        if not is_left_task and not is_right_task:
            result_signature = op_task.s(left_operand, right_operand) # DÒNG THÊM MỚI
            logger.info(f"  [CASE 1: SIMPLE] Tạo signature đơn giản: {result_signature}") # DÒNG THÊM MỚI
            return result_signature # DÒNG THAY ĐỔI

        # 2. Nhánh trái là task, nhánh phải là số (ví dụ: (2+3) * 4) -> SEQUENTIAL
        elif is_left_task and not is_right_task:
            result_signature = chain( # DÒNG THÊM MỚI
                left_operand, 
                combine_and_operate.s(
                    operation_name=node.operation, 
                    fixed_operand=right_operand, 
                    is_left_fixed=False
                )
            )
            logger.info(f"  [CASE 2: SEQUENTIAL L] Tạo chain: {result_signature}") # DÒNG THÊM MỚI
            return result_signature # DÒNG THAY ĐỔI
            
        # 3. Nhánh trái là số, nhánh phải là task (ví dụ: 5 - (1+2)) -> SEQUENTIAL
        elif not is_left_task and is_right_task:
            result_signature = chain( # DÒNG THÊM MỚI
                right_operand, 
                combine_and_operate.s(
                    operation_name=node.operation, 
                    fixed_operand=left_operand, 
                    is_left_fixed=True
                )
            )
            logger.info(f"  [CASE 3: SEQUENTIAL R] Tạo chain: {result_signature}") # DÒNG THÊM MỚI
            return result_signature # DÒNG THAY ĐỔI

        # 4. Cả hai nhánh đều là task (ví dụ: (2+3) + (4*5)) -> PARALLEL / HYBRID
        else:
            # Chạy cả hai nhánh song song bằng `group`
            parallel_tasks = group(left_operand, right_operand)
            logger.info(f"  [CASE 4: HYBRID/PARALLEL] Tạo group song song: {parallel_tasks}") # DÒNG THÊM MỚI
            # Sau đó dùng `combine_and_operate` để kết hợp kết quả
            # chain(group, combiner.s(op)) -> combiner([left_res, right_res], op) -> op(left_res, right_res)
            result_signature = chain(parallel_tasks, combine_and_operate.s(node.operation)) # DÒNG THÊM MỚI
            logger.info(f"  [CASE 4: HYBRID/PARALLEL] Tạo chain cuối cùng: {result_signature}") # DÒNG THÊM MỚI
            return result_signature # DÒNG THAY ĐỔI