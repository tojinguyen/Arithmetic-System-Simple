import logging
from typing import Dict, Any

from .add_service import add as add_task
from .mul_service import multiply as mul_task
from .div_service import divide as div_task
from .sub_service import subtract as sub_task

from .expression_parser import ExpressionParser, Operations, ExpressionType
from .workflow_builder import WorkflowBuilder 

logger = logging.getLogger(__name__)

class WorkflowOrchestrator:
    """
    Điều phối toàn bộ quá trình tính toán:
    1. Phân tích (Parse) chuỗi biểu thức thành cây cú pháp (AST).
    2. Sử dụng WorkflowBuilder để xây dựng một workflow Celery (chain, group,...) từ cây.
    3. Thực thi workflow và trả về kết quả.
    """
    def __init__(self):
        # Khởi tạo đầy đủ task map để truyền cho builder
        self.task_map = {
            Operations.ADD: add_task,
            Operations.SUB: sub_task,
            Operations.MUL: mul_task,
            Operations.DIV: div_task
        }
        self.parser = ExpressionParser()
        # Sử dụng WorkflowBuilder thay cho Analyzer và Executor cũ
        self.builder = WorkflowBuilder(self.task_map)

    def calculate(self, expression: str) -> Dict[str, Any]:
        try:
            logger.info(f"=== Bắt đầu tính toán cho biểu thức: '{expression}' ===")

            # Bước 1: Phân tích (parse) biểu thức thành cây cú pháp
            logger.info("Phân tích (parsing) biểu thức...")
            parsed = self.parser.parse(expression)
            logger.info("Phân tích hoàn tất.")

            # ---- PHẦN SỬA LỖI ----
            # Xử lý trường hợp đặc biệt: biểu thức chỉ là một con số.
            # Parser của bạn đã thông minh khi trả về một float nếu biểu thức là số.
            if isinstance(parsed.expression_tree, (int, float)):
                logger.info("Phát hiện biểu thức là một con số đơn giản, không cần Celery.")
                final_result = parsed.expression_tree
                # Trả về kết quả ngay lập tức
                return {
                    "result": final_result,
                    "expression_type": ExpressionType.SIMPLE.value,
                    "original_expression": expression,
                }
            # ---------------------

            # Bước 2: Xây dựng và thực thi workflow cho các biểu thức phức tạp
            logger.info("Xây dựng workflow Celery từ cây cú pháp...")
            # self.builder.build sẽ trả về một đối tượng AsyncResult
            celery_result = self.builder.build(parsed.expression_tree)
            logger.info(f"Workflow đã được tạo và gửi đi với ID tác vụ: {celery_result.id}")

            # Bước 3: Đợi và lấy kết quả cuối cùng
            logger.info("Đợi kết quả từ Celery...")
            final_result = celery_result.get(timeout=30)
            logger.info(f"Tính toán hoàn tất! Kết quả cuối cùng: {final_result}")

            # Bước 4: Trả về kết quả
            return {
                "result": final_result,
                "expression_type": ExpressionType.HYBRID.value,
                "original_expression": expression,
            }
        except Exception as e:
            logger.error(f"Lỗi nghiêm trọng trong quá trình tính toán cho '{expression}': {str(e)}", exc_info=True)
            raise ValueError(f"Không thể xử lý biểu thức: {str(e)}")