from pydantic import BaseModel, Field
from typing import Optional, Any, List
from enum import Enum

class ExpressionTypeEnum(str, Enum):
    SIMPLE = "simple"
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    HYBRID = "hybrid"

class CalculateExpressionResponse(BaseModel):
    result: float = Field(..., description="Calculation result")
    original_expression: str = Field(..., description="Original expression provided")
