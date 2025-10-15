from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    code: int = Field(..., description="Error code")
    message: str = Field(..., description="Error message")


class CalculateExpressionResponse(BaseModel):
    result: float = Field(..., description="Calculation result")
    workflow: str = Field(
        ..., description="The Celery workflow structure used for the calculation."
    )
