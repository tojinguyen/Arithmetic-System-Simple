import asyncio
from ..models.worker_models import CalculatorInput, CalculatorOutput
from ..config import BROKER, RESULT_BACKEND
from ..constants.constants import ADD_TASKS_TOPIC
import logging
from mini.worker.workers import Worker

logger = logging.getLogger(__name__)

class AddWorker(Worker[CalculatorInput, CalculatorOutput]):
    Input = CalculatorInput
    Output = CalculatorOutput

    async def before_start(self, input_obj: CalculatorInput) -> None:
        logger.info(f"Before start: {input_obj}")

    async def on_success(self, input_obj: CalculatorInput, result: CalculatorOutput) -> None:
        logger.info(f"Task succeeded: {input_obj} -> {result}")

    async def on_failure(self, input_obj: CalculatorInput, exc: Exception) -> None:
        logger.error(f"Task failed: {input_obj} with exception {exc}", exc_info=True)

    async def process(self, input_obj: CalculatorInput) -> CalculatorOutput:
        logger.info(f"Task started: {input_obj}")
        if input_obj.result is not None:
            result = input_obj.result + input_obj.y
        else:
            result = input_obj.x + input_obj.y
        return CalculatorOutput(result=result)

    async def sent_result(self, topic: str, input_obj: CalculatorInput) -> None:
        logger.info(f"Result sent to {topic}: {input_obj}")

async def main():
    add_worker = AddWorker(BROKER, ADD_TASKS_TOPIC, RESULT_BACKEND)
    await add_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())