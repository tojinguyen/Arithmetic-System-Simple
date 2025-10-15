import logging
from typing import Dict, Any
import asyncio
from ..config import BROKER, RESULT_BACKEND
from .expression_parser import ExpressionParser
from .workflow_builder import WorkflowBuilder

logger = logging.getLogger(__name__)

class WorkflowOrchestrator:
    def __init__(self):
        self.parser = ExpressionParser()
        self.builder = WorkflowBuilder()

    def calculate(self, expression: str) -> Dict[str, Any]:
        try:
            parsed = self.parser.parse(expression)
            workflow = self.builder.build(parsed.expression_tree)
            if isinstance(workflow, float):
                final_result = workflow
            else:
                final_result = asyncio.run(self._execute_workflow(workflow))
            logger.info(f"Final Result: {final_result}")

            return {
                "result": final_result,
                "original_expression": expression,
            }
        except Exception as e:
            logger.error(f"Error while calculating '{expression}': {str(e)}", exc_info=True)
            raise ValueError(f"Cannot calculate expression: {expression}. Error: {str(e)}")

    async def _execute_workflow(self, workflow):
        await workflow.create(result_backend=RESULT_BACKEND)
        await workflow.start(broker=BROKER)

        logger.info(f"Started workflow with ID: {workflow.id}")
        result_node = await RESULT_BACKEND.get_result(workflow.id)
        while result_node is None:
            await asyncio.sleep(0.1)
            result_node = await RESULT_BACKEND.get_result(workflow.id)

        return result_node.result_obj.get('value')