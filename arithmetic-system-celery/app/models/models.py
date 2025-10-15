from pydantic import BaseModel, Field


class CalculateExpressionResponse(BaseModel):
    result: float = Field(..., description="Calculation result")
    workflow: str = Field(
        ..., description="The Celery workflow structure used for the calculation."
    )
