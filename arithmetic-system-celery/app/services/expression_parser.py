from __future__ import annotations
import re
import ast
import logging
from dataclasses import dataclass
from enum import Enum, auto
from app.types.errors import (
    ExpressionSyntaxError,
    UnsupportedOperatorError,
    UnsupportedNodeError,
    UnsupportedUnaryOperatorError,
    ComplexUnaryExpressionError,
)

logger = logging.getLogger(__name__)

REGEX_SPACES = re.compile(r"\s+")
REGEX_VALID_CHARACTERS = re.compile(r"^[0-9+\-*/().%\s]+$")


class OperationEnum(Enum):
    ADD = auto()
    SUB = auto()
    MUL = auto()
    DIV = auto()

    @property
    def is_commutative(self) -> bool:
        return self in {OperationEnum.ADD, OperationEnum.MUL}


@dataclass
class ExpressionNode:
    operation: OperationEnum
    left: ExpressionNode | float
    right: ExpressionNode | float

    def log_tree(self, indent: int = 0, prefix: str = "") -> str:
        result = []
        current_indent = "  " * indent

        # Current node
        op_symbol = self._get_operation_symbol()
        result.append(f"{current_indent}{prefix}{op_symbol}")

        # Left child
        if isinstance(self.left, ExpressionNode):
            result.append(self.left.log_tree(indent + 1, "├── "))
        else:
            result.append(f"{current_indent}  ├── {self.left}")

        # Right child
        if isinstance(self.right, ExpressionNode):
            result.append(self.right.log_tree(indent + 1, "└── "))
        else:
            result.append(f"{current_indent}  └── {self.right}")

        return "\n".join(result)

    def _get_operation_symbol(self) -> str:
        symbols = {
            OperationEnum.ADD: "+",
            OperationEnum.SUB: "-",
            OperationEnum.MUL: "*",
            OperationEnum.DIV: "/",
        }
        return symbols.get(self.operation, "?")

    def __str__(self) -> str:
        return self.log_tree()


class ExpressionParser:
    OPERATORS = {
        ast.Add: OperationEnum.ADD,
        ast.Sub: OperationEnum.SUB,
        ast.Mult: OperationEnum.MUL,
        ast.Div: OperationEnum.DIV,
    }

    def __init__(self):
        self.operations = []

    def parse(self, expression: str) -> ExpressionNode | float | int:
        clean_expr = self._clean_expression(expression)
        try:
            tree = ast.parse(clean_expr, mode="eval")
        except SyntaxError as e:
            raise ExpressionSyntaxError(expression, str(e)) from e
        expr_tree = self._build_expression_tree(tree.body)
        logger.info(f"Parsed Expression Tree:\n{expr_tree}")

        return expr_tree

    def _build_expression_tree(self, node) -> ExpressionNode | float | int:
        if isinstance(node, ast.BinOp):
            op_symbol = self.OPERATORS.get(type(node.op))
            if op_symbol is None:
                raise UnsupportedOperatorError(str(type(node.op).__name__))

            left = self._build_expression_tree(node.left)
            right = self._build_expression_tree(node.right)

            return ExpressionNode(operation=op_symbol, left=left, right=right)

        if isinstance(node, ast.Constant):
            return node.value

        if not isinstance(node, ast.UnaryOp):
            raise UnsupportedNodeError(type(node).__name__)

        if not isinstance(node.op, ast.USub):
            raise UnsupportedUnaryOperatorError(type(node.op).__name__)

        operand = self._build_expression_tree(node.operand)
        if isinstance(operand, (int, float)):
            return -operand

        raise ComplexUnaryExpressionError(str(node))

    def _clean_expression(self, expression: str) -> str:
        clean = REGEX_SPACES.sub("", expression)
        if not clean:
            raise ExpressionSyntaxError(expression, "Expression cannot be empty")

        if not REGEX_VALID_CHARACTERS.match(clean):
            raise ExpressionSyntaxError(
                expression, "Expression contains invalid characters"
            )
        return clean
