import logging
from typing import Dict, Any

from ..workers.add_service import add as add_task
from ..workers.mul_service import multiply as mul_task
from ..workers.div_service import divide as div_task
from ..workers.sub_service import subtract as sub_task

from .expression_parser import ExpressionParser, OperationEnum
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
            parsed = self.parser.parse(expression)
            workflow_result = self.builder.build(parsed.expression_tree)
            final_result = workflow_result.get(timeout=30)
            logger.info(f"Final Result: {final_result}")

            return {
                "result": final_result,
                "original_expression": expression,
            }
        except Exception as e:
            logger.error(f"Error while calculating '{expression}': {str(e)}", exc_info=True)
            raise ValueError(f"Cannot calculate expression: {expression}. Error: {str(e)}")