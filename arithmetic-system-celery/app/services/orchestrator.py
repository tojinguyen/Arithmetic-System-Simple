import logging

from ..workers.add_service import add as add_task
from ..workers.mul_service import multiply as mul_task
from ..workers.div_service import divide as div_task
from ..workers.sub_service import subtract as sub_task

from .expression_parser import ExpressionParser, OperationEnum
from .workflow_builder import WorkflowBuilder
from ..models.models import CalculateExpressionResponse

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

    def calculate(self, expression: str) -> CalculateExpressionResponse:
        parsed = self.parser.parse(expression)
        workflow_async_result, workflow_str = self.builder.build(parsed)
        final_result = workflow_async_result.get(timeout=3)
        logger.info(f"Final Result: {final_result}")

        return CalculateExpressionResponse(result=final_result, workflow=workflow_str)
