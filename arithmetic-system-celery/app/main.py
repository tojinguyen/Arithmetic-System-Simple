from fastapi import FastAPI
from .api.calculate_expression import router as evaluate_router
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

app = FastAPI()

app.include_router(evaluate_router, prefix="/api")
