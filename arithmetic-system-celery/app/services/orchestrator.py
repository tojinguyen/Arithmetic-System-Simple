import logging

from app.workers import (
    add_task,
    subtract_task,
    multiply_task,
    divide_task,
    subtract_list_task,
    divide_list_task,
)

from .expression_parser import ExpressionParser, OperationEnum
from .workflow_builder import WorkflowBuilder
from app.models.models import CalculateExpressionResponse
from typing import Callable

logger = logging.getLogger(__name__)


class WorkflowOrchestrator:
    def __init__(self):
        self.task_map: dict[OperationEnum, Callable[..., int | float]] = {
            OperationEnum.ADD: add_task,
            OperationEnum.SUB: subtract_task,
            OperationEnum.MUL: multiply_task,
            OperationEnum.DIV: divide_task,
        }

        self.task_map_chord: dict[OperationEnum, Callable[..., int | float]] = {
            OperationEnum.SUB: subtract_list_task,
            OperationEnum.DIV: divide_list_task,
        }

        self.parser = ExpressionParser()
        self.builder = WorkflowBuilder(self.task_map, self.task_map_chord)

    def calculate(self, expression: str) -> CalculateExpressionResponse:
        parsed = self.parser.parse(expression)
        workflow_async_result, workflow_str = self.builder.build(parsed)
        final_result = workflow_async_result.get(timeout=3)
        logging.info(f"Workflow String: {workflow_str}")
        logger.info(f"Final Result: {final_result}")

        return CalculateExpressionResponse(result=final_result, workflow=workflow_str)
