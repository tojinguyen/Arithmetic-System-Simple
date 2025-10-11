from fastapi import APIRouter, Query, HTTPException
import logging
from ..services.orchestrator import WorkflowOrchestrator
from ..models.models import CalculateExpressionResponse

router = APIRouter()
logger = logging.getLogger(__name__)
orchestrator = WorkflowOrchestrator()

@router.get("/calculate", response_model=CalculateExpressionResponse)
def evaluate(expression: str = Query(..., description="Arithmetic expression to evaluate")):
    try:
        logger.info(f"Received expression to evaluate: {expression}")
        result = orchestrator.calculate(expression)
        return CalculateExpressionResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
