import logging
from typing import Dict, Any

from .add_service import add as add_task
from .mul_service import multiply as mul_task
from .div_service import divide as div_task
from .sub_service import subtract as sub_task

from .expression_parser import ExpressionParser, OperationEnum, ExpressionType
from .workflow_builder import WorkflowBuilder 

logger = logging.getLogger(__name__)

class WorkflowOrchestrator:
    def __init__(self):
        self.task_map = {
            OperationEnum.ADD: add_task,
            OperationEnum.SUB: sub_task,
            OperationEnum.MUL: mul_task,
            OperationEnum.DIV: div_task,
        }
        self.parser = ExpressionParser()
        self.builder = WorkflowBuilder(self.task_map)

    def calculate(self, expression: str) -> Dict[str, Any]:
        try:
            logger.info(f"=== Bắt đầu tính toán cho biểu thức: '{expression}' ===")
            parsed = self.parser.parse(expression)

            if isinstance(parsed.expression_tree, (int, float)):
                final_result = parsed.expression_tree
                return {
                    "result": final_result,
                    "expression_type": ExpressionType.SIMPLE.value,
                    "original_expression": expression,
                }

            logger.info("Xây dựng workflow Celery từ cây cú pháp...")
            workflow_result, metadata = self.builder.build(parsed.expression_tree)
            logger.info(f"Workflow đã được tạo và gửi đi với ID tác vụ: {workflow_result.id}")

            logger.info("Đợi kết quả từ Celery...")
            final_result = workflow_result.get(timeout=30)
            logger.info(f"Tính toán hoàn tất! Kết quả cuối cùng: {final_result}")

            return {
                "result": final_result,
                "expression_type": metadata["type"],
                "original_expression": expression,
                "parallel_groups_count": metadata["groups"],
                "sequential_chains_count": metadata["chains"],
            }
        except Exception as e:
            logger.error(f"Lỗi nghiêm trọng trong quá trình tính toán cho '{expression}': {str(e)}", exc_info=True)
            raise ValueError(f"Không thể xử lý biểu thức: {str(e)}")