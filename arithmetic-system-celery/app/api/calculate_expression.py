from fastapi import APIRouter, Query
import logging
from ..services.orchestrator import WorkflowOrchestrator
from ..models.models import CalculateExpressionResponse, ErrorResponse
from http import HTTPStatus
from app.types.errors import (
    ExpressionSyntaxError,
    UnsupportedOperatorError,
    UnsupportedNodeError,
    UnsupportedUnaryOperatorError,
    ComplexUnaryExpressionError,
)

router = APIRouter()
logger = logging.getLogger(__name__)
orchestrator = WorkflowOrchestrator()


@router.get("/calculate", response_model=CalculateExpressionResponse)
def evaluate(
    expression: str = Query(..., description="Arithmetic expression to evaluate"),
) -> CalculateExpressionResponse:
    try:
        logger.info(f"Received expression to evaluate: {expression}")
        result = orchestrator.calculate(expression)
        return result
    except ExpressionSyntaxError as e:
        logger.error(f"Syntax error in expression '{expression}': {str(e)}")
        raise ErrorResponse(code=HTTPStatus.BAD_REQUEST, message=str(e))
    except (
        UnsupportedOperatorError,
        UnsupportedNodeError,
        UnsupportedUnaryOperatorError,
        ComplexUnaryExpressionError,
    ) as e:
        logger.error(f"Unsupported operation in expression '{expression}': {str(e)}")
        raise ErrorResponse(code=HTTPStatus.BAD_REQUEST, message=str(e))
