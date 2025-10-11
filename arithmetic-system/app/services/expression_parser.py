import re
import ast
import logging
from typing import Union, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class ExpressionType(Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    HYBRID = "hybrid"
    SIMPLE = "simple"

class Operations:
    ADD = "add"
    SUB = "sub"
    MUL = "mul"
    DIV = "div"

    # List of all operations for easy iteration
    ALL = [ADD, SUB, MUL, DIV]

    # Commutative operations (order doesn't matter)
    COMMUTATIVE = [ADD, MUL]

    # Non-commutative operations (order matters)
    NON_COMMUTATIVE = [SUB, DIV]

# Operation constants to avoid magic strings
class Operations:
    ADD = "add"
    SUB = "sub"
    MUL = "mul"
    DIV = "div"

    # List of all operations for easy iteration
    ALL = [ADD, SUB, MUL, DIV]

    # Commutative operations (order doesn't matter)
    COMMUTATIVE = [ADD, MUL]

    # Non-commutative operations (order matters)
    NON_COMMUTATIVE = [SUB, DIV]


@dataclass
class Operation:
    operation: str
    operand1: Union[float, str, 'ExpressionNode']
    operand2: Union[float, str, 'ExpressionNode']


@dataclass
class ExpressionNode:
    operation: str
    left: Union[float, 'ExpressionNode']
    right: Union[float, 'ExpressionNode']
    level: int = 0


@dataclass
class ParsedExpression:
    expression_type: ExpressionType
    expression_tree: Optional[ExpressionNode]
    original_expression: str


class ExpressionParser:
    OPERATORS = {
        '+': Operations.ADD,
        '-': Operations.SUB,
        '*': Operations.MUL,
        '/': Operations.DIV
    }

    def __init__(self):
        self.operations = []
        self.parallel_groups = []
        self.sequential_chains = []

    def _log_tree_structure(self, tree: Union[ExpressionNode, float], prefix: str = "", is_last: bool = True,
                            depth: int = 0):
        if depth == 0:
            logger.info("EXPRESSION TREE STRUCTURE:")
            logger.info("=" * 50)

        if isinstance(tree, ExpressionNode):
            connector = "└── " if is_last else "├── "
            logger.info(f"{prefix}{connector}[{tree.operation.upper()}] (Level: {tree.level})")

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

    def parse(self, expression: str) -> ParsedExpression:
        logger.info("=" * 80)
        logger.info("STARTING EXPRESSION PARSING")
        logger.info("=" * 80)
        logger.info(f"Input expression: {expression}")

        try:
            # Clean the expression
            logger.info("-" * 40)
            logger.info("STEP 1: Cleaning expression")
            clean_expr = self._clean_expression(expression)
            logger.info(f"Cleaned expression: {clean_expr}")

            # Parse as AST
            logger.info("\n\n")
            logger.info("-" * 40)
            logger.info("STEP 2: Parsing AST")
            tree = ast.parse(clean_expr, mode='eval')
            logger.info(f"AST created successfully for: {clean_expr}")
            logger.info(f"This AST tree structure: {ast.dump(tree)}")

            # Build expression tree
            logger.info("\n\n")
            logger.info("-" * 40)
            logger.info("STEP 3: Building expression tree")
            expr_tree = self._build_expression_tree(tree.body, level=0)
            logger.info("Expression tree built successfully")
            self._log_tree_structure(expr_tree, "ROOT")

            result = ParsedExpression(
                expression_type=ExpressionType.SEQUENTIAL,
                expression_tree=expr_tree,
                original_expression=expression
            )

            logger.info("=" * 80)
            logger.info("PARSING COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)

            return result

        except Exception as e:
            logger.error("=" * 80)
            logger.error("PARSING FAILED")
            logger.error("=" * 80)
            logger.error(f"Error: {str(e)}")
            raise ValueError(f"Invalid expression: {expression}. Error: {str(e)}")

    def _build_expression_tree(self, node, level: int = 0) -> Union[ExpressionNode, float]:
        if isinstance(node, ast.BinOp):
            op_symbol = self._get_operator_symbol(node.op)

            if op_symbol not in self.OPERATORS:
                logger.error(f"Unsupported operator: {op_symbol}")
                raise ValueError(f"Unsupported operator: {op_symbol}")

            left = self._build_expression_tree(node.left, level + 1)
            right = self._build_expression_tree(node.right, level + 1)

            result = ExpressionNode(
                operation=self.OPERATORS[op_symbol],
                left=left,
                right=right,
                level=level
            )

            return result

        elif isinstance(node, ast.Constant):
            value = float(node.value)
            return value
        else:
            logger.error(f"Unsupported node type: {type(node)}")
            raise ValueError(f"Unsupported node type: {type(node)}")

    def _clean_expression(self, expression: str) -> str:
        logger.info("CLEAN_EXPRESSION FUNCTION")
        logger.info(f"Original expression: '{expression}'")

        # Remove whitespace
        clean = re.sub(r'\s+', '', expression)
        logger.info(f"After removing whitespace: '{clean}'")

        # Validate characters (numbers, operators, parentheses, decimal points)
        if not re.match(r'^[0-9+\-*/().]+$', clean):
            logger.error(f"Expression contains invalid characters: '{clean}'")
            raise ValueError("Expression contains invalid characters")

        logger.info(f"Expression validation passed: '{clean}'")
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
