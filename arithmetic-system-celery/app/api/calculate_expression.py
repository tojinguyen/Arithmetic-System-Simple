from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
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
) -> CalculateExpressionResponse | JSONResponse:
    try:
        logger.info(f"Received expression to evaluate: {expression}")
        result = orchestrator.calculate(expression)
        return result

    except ExpressionSyntaxError as e:
        logger.error(f"Syntax error in expression '{expression}': {str(e)}")
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content=ErrorResponse(
                code=HTTPStatus.BAD_REQUEST, message=str(e)
            ).model_dump(),
        )

    except (
        UnsupportedOperatorError,
        UnsupportedNodeError,
        UnsupportedUnaryOperatorError,
        ComplexUnaryExpressionError,
    ) as e:
        logger.error(f"Unsupported operation in expression '{expression}': {str(e)}")
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content=ErrorResponse(
                code=HTTPStatus.BAD_REQUEST, message=str(e)
            ).model_dump(),
        )

    except ZeroDivisionError as e:
        logger.error(f"Division by zero in expression '{expression}': {str(e)}")
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content=ErrorResponse(
                code=HTTPStatus.BAD_REQUEST, message="Cannot divide by zero"
            ).model_dump(),
        )

    except Exception as e:
        logger.error(f"Unexpected error while evaluating '{expression}': {str(e)}")
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content=ErrorResponse(
                code=HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An unexpected error occurred",
            ).model_dump(),
        )
