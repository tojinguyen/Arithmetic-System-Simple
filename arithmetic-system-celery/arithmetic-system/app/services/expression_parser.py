import re
import ast
import logging
from typing import Union, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class OperationEnum(str, Enum):
    ADD = "add"
    SUB = "sub"
    MUL = "mul"
    DIV = "div"

    @property
    def is_commutative(self):
        return self in {OperationEnum.ADD, OperationEnum.MUL}


@dataclass
class ExpressionNode:
    operation: OperationEnum
    left: Union[float, 'ExpressionNode']
    right: Union[float, 'ExpressionNode']

@dataclass
class ParsedExpression:
    expression_tree: Union[ExpressionNode, float, None]
    original_expression: str


class ExpressionParser:
    OPERATORS = {
        '+': OperationEnum.ADD,
        '-': OperationEnum.SUB,
        '*': OperationEnum.MUL,
        '/': OperationEnum.DIV
    }

    def __init__(self):
        self.operations = []
        self.parallel_groups = []
        self.sequential_chains = []

    def parse(self, expression: str) -> ParsedExpression:
        logger.info("=" * 80)
        logger.info(f"Input expression: {expression}")

        try:
            clean_expr = self._clean_expression(expression)
            tree = ast.parse(clean_expr, mode='eval')
            expr_tree = self._build_expression_tree(tree.body, level=0)
            self._log_tree_structure(expr_tree)

            result = ParsedExpression(
                expression_tree=expr_tree,
                original_expression=expression
            )

            return result
        except Exception as e:
            raise ValueError(f"Invalid expression: {expression}. Error: {str(e)}")

    def _build_expression_tree(self, node, level: int = 0) -> Union[ExpressionNode, float]:
        if isinstance(node, ast.BinOp):
            op_symbol = self._get_operator_symbol(node.op)

            if op_symbol not in self.OPERATORS:
                raise ValueError(f"Unsupported operator: {op_symbol}")

            left = self._build_expression_tree(node.left, level + 1)
            right = self._build_expression_tree(node.right, level + 1)

            result = ExpressionNode(
                operation=self.OPERATORS[op_symbol],
                left=left,
                right=right
            )

            return result

        elif isinstance(node, ast.Constant):
            value = float(node.value)
            return value
        elif isinstance(node, ast.UnaryOp):
            if isinstance(node.op, ast.USub):
                operand = self._build_expression_tree(node.operand, level)
                if isinstance(operand, (int, float)):
                    return -operand
                else:
                    raise ValueError("Unary subtraction on complex expression is not supported")
            else:
                raise ValueError(f"Unsupported unary operator: {type(node.op)}")
        else:
            logger.error(f"Unsupported node type: {type(node)}")
            raise ValueError(f"Unsupported node type: {type(node)}")

    def _clean_expression(self, expression: str) -> str:
        clean = re.sub(r'\s+', '', expression)

        if not re.match(r'^[0-9+*/().%-]+$', clean):
            raise ValueError("Expression contains invalid characters")
        return clean

    def _get_operator_symbol(self, op) -> str:
        op_map = {
            ast.Add: '+',
            ast.Sub: '-',
            ast.Mult: '*',
            ast.Div: '/'
        }
        symbol = op_map.get(type(op), '?')
        return symbol

    def _log_tree_structure(self, tree: Union[ExpressionNode, float], prefix: str = "", is_last: bool = True,
                            depth: int = 0):
        if depth == 0:
            logger.info("EXPRESSION TREE STRUCTURE:")
            logger.info("=" * 50)

        if isinstance(tree, ExpressionNode):
            connector = "└── " if is_last else "├── "
            logger.info(f"{prefix}{connector}[{tree.operation.upper()}]")

            child_prefix = prefix + ("    " if is_last else "│   ")

            if isinstance(tree.left, ExpressionNode):
                self._log_tree_structure(tree.left, child_prefix, False, depth + 1)
            else:
                logger.info(f"{child_prefix}├── {tree.left} (leaf)")

            if isinstance(tree.right, ExpressionNode):
                self._log_tree_structure(tree.right, child_prefix, True, depth + 1)
            else:
                logger.info(f"{child_prefix}└── {tree.right} (leaf)")
        else:
            connector = "└── " if is_last else "├── "
            logger.info(f"{prefix}{connector}{tree} (leaf)")

        if depth == 0:
            logger.info("=" * 50)
