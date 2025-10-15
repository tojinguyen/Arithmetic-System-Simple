import asyncio
from math import fsum

from mini.worker.workers import Worker
from ..models.worker_models import AggregatorInput, CalculatorOutput
from ..config import BROKER, RESULT_BACKEND
from ..constants.constants import XSUM_TASKS_TOPIC

class XSumWorker(Worker[AggregatorInput, CalculatorOutput]):
    Input = AggregatorInput
    Output = CalculatorOutput

    async def before_start(self, input_obj: AggregatorInput) -> None:
        pass

    async def on_success(self, input_obj: AggregatorInput, result: CalculatorOutput) -> None:
        pass

    async def on_failure(self, input_obj: AggregatorInput, exc: Exception) -> None:
        pass

    async def process(self, input_obj: AggregatorInput) -> CalculatorOutput:
        values_to_sum = []
        if input_obj.children_result:
            for child_res in input_obj.children_result:
                if isinstance(child_res, dict) and 'result' in child_res:
                    values_to_sum.append(child_res['result'])

        if input_obj.constants:
            values_to_sum.extend(input_obj.constants)

        result = fsum(values_to_sum)
        return CalculatorOutput(result=result)

    async def sent_result(self, topic: str, input_obj: CalculatorOutput) -> None:
        pass

async def main():
    xsum_worker = XSumWorker(BROKER, XSUM_TASKS_TOPIC, RESULT_BACKEND)
    await xsum_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())