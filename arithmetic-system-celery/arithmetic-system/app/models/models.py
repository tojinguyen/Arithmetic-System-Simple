from pydantic import BaseModel, Field

class CalculateExpressionResponse(BaseModel):
    result: float = Field(..., description="Calculation result")
    original_expression: str = Field(..., description="Original expression provided")
