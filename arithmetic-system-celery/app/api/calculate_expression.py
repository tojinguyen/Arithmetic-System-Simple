from fastapi import APIRouter, Query, HTTPException
import logging
from ..services.orchestrator import WorkflowOrchestrator
from ..models.models import CalculateExpressionResponse
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
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(e))

    except (
        UnsupportedOperatorError,
        UnsupportedNodeError,
        UnsupportedUnaryOperatorError,
        ComplexUnaryExpressionError,
    ) as e:
        logger.error(f"Unsupported operation in expression '{expression}': {str(e)}")
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(e))

    except ZeroDivisionError as e:
        logger.error(f"Division by zero in expression '{expression}': {str(e)}")
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail="Cannot divide by zero"
        )

    except Exception as e:
        logger.error(f"Unexpected error while evaluating '{expression}': {str(e)}")
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
        )
