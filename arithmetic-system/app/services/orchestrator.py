from celery.result import AsyncResult
from typing import Dict, Any
import logging

from .add_service import add as add_task
from .mul_service import multiply as mul_task
from .div_service import divide as div_task
from .sub_service import subtract as sub_task

from .expression_parser import (
    ExpressionParser, ExpressionType, Operations
)
from .expression_analyzer import ExpressionAnalyzer
from .task_executor import TaskExecutor

logger = logging.getLogger(__name__)


class WorkflowOrchestrator:
    def __init__(self):
        self.task_map = {
            Operations.ADD: add_task,
            Operations.SUB: sub_task,
            Operations.MUL: mul_task,
            Operations.DIV: div_task
        }
        self.parser = ExpressionParser()
        self.analyzer = ExpressionAnalyzer(self.task_map)
        self.executor = TaskExecutor(self.task_map)

    def calculate(self, expression: str) -> Dict[str, Any]:
        try:
            logger.info(f"=== Starting calculation for expression: '{expression}' ===")

            logger.info("Step 1: Parsing expression...")
            parsed = self.parser.parse(expression)

            logger.info("\n\n\n\n")
            logger.info("Step 2: Analyzing expression structure...")
            parsed = self.analyzer.determine_expression_type(parsed)
            logger.info(f"Expression analysis complete - Final type: {parsed.expression_type}")

            logger.info("\n\n\n\n")
            logger.info("Step 3: Selecting execution strategy...")
            celery_result = self._execute_expression(parsed)

            logger.info("\n\n\n\n")
            final_result = celery_result.get(timeout=30)

            logger.info("\n\n\n\n")
            logger.info(f"Step 4: Calculation complete! Final result: {final_result}")

            return {
                "result": final_result,
                "expression_type": parsed.expression_type.value,
                "original_expression": expression,
            }

        except Exception as e:
            logger.error(f"Error in calculate_sync for '{expression}': {str(e)}")
            raise ValueError(f"Failed to process expression: {str(e)}")

    def _execute_expression(self, parsed):
        expression_type = parsed.expression_type

        if expression_type == ExpressionType.SIMPLE:
            return self.executor.execute_simple(parsed)

        elif expression_type == ExpressionType.SEQUENTIAL:
            return self.executor.execute_sequential(parsed)

        elif expression_type == ExpressionType.PARALLEL:
            parallel_tasks = self.analyzer.extract_parallel_tasks_from_expression(parsed.expression_tree)
            logger.info(f"Parallel tasks extracted: {parallel_tasks}")
            return self.executor.execute_parallel(parsed, parallel_tasks)

        elif expression_type == ExpressionType.HYBRID:
            parallel_tasks = self.analyzer.extract_parallel_tasks_from_expression(parsed.expression_tree)
            logger.info(f"Parallel tasks extracted for hybrid: {parallel_tasks}")
            final_op, final_operand = self.analyzer.extract_final_operation(parsed.expression_tree)
            return self.executor.execute_hybrid(parsed, parallel_tasks, final_op, final_operand)

        else:
            raise ValueError(f"Unsupported expression type: {expression_type}")