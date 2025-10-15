import asyncio
from ..models.worker_models import CalculatorInput, CalculatorOutput
from ..config import BROKER, RESULT_BACKEND
from ..constants.constants import SUB_TASKS_TOPIC
from mini.worker.workers import Worker

class SubWorker(Worker[CalculatorInput, CalculatorOutput]):
    Input = CalculatorInput
    Output = CalculatorOutput

    async def before_start(self, input_obj: CalculatorInput) -> None:
        pass

    async def on_success(self, input_obj: CalculatorInput, result: CalculatorOutput) -> None:
        pass

    async def on_failure(self, input_obj: CalculatorInput, exc: Exception) -> None:
        pass

    async def process(self, input_obj: CalculatorInput) -> CalculatorOutput:
        if input_obj.result is not None:
            if input_obj.is_left_fixed:
                result = input_obj.x - input_obj.result
            else:
                result = input_obj.result - input_obj.y
        else:
            result = input_obj.x - input_obj.y

        return CalculatorOutput(result=result)

    async def sent_result(self, topic: str, input_obj: CalculatorOutput) -> None:
        pass

async def main():
    sub_worker = SubWorker(BROKER, SUB_TASKS_TOPIC, RESULT_BACKEND)
    await sub_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())