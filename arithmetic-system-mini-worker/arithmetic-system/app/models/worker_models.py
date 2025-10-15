from pydantic import BaseModel
from typing import List, Any


class CalculatorInput(BaseModel):
    result: float | None = None
    x: float | None = None
    y: float | None = None
    is_left_fixed: bool = False

class CalculatorOutput(BaseModel):
    result: float

class AggregatorInput(BaseModel):
    children_result: List[Any] | None = None
    constants: List[float] | None = None