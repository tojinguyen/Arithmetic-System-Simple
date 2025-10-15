from pydantic import BaseModel, Field


class CalculateExpressionResponse(BaseModel):
    result: float = Field(..., description="Calculation result")
