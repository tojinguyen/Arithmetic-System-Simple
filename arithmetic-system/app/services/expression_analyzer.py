from typing import Dict, Any, List, Union, Tuple, Optional
import logging

from .expression_parser import ExpressionNode, ParsedExpression, ExpressionType, Operations

logger = logging.getLogger(__name__)

class ExpressionAnalyzer:
    def __init__(self, task_map=None):
        self.task_map = task_map or {}

    def determine_expression_type(self, parsed_expression: ParsedExpression) -> ParsedExpression:
        if not parsed_expression.expression_tree:
            logger.info("Expression type determined: SEQUENTIAL (no expression tree)")
            return parsed_expression

        tree = parsed_expression.expression_tree

        if isinstance(tree, (int, float)):
            parsed_expression.expression_type = ExpressionType.SIMPLE
            logger.info("Expression type determined: SIMPLE (single number)")
            return parsed_expression

        if isinstance(tree, ExpressionNode):
            if (isinstance(tree.left, (int, float)) and isinstance(tree.right, (int, float))):
                parsed_expression.expression_type = ExpressionType.SIMPLE
                logger.info("Expression type determined: SIMPLE (single operation with two numbers)")
                return parsed_expression

        parallel_tasks = self.extract_parallel_tasks_from_expression(parsed_expression.expression_tree)
        is_fully_commutative = self.is_commutative_expression(parsed_expression.expression_tree)
        has_parallel_opportunities = len(parallel_tasks) >= 2

        if has_parallel_opportunities:
            if is_fully_commutative:
                parsed_expression.expression_type = ExpressionType.PARALLEL
                logger.info(
                    f"Expression type determined: PARALLEL ({len(parallel_tasks)} parallel tasks, fully commutative)")
            # TODO: Revisit this logic for hybrid classification
            # else:
            #     parsed_expression.expression_type = ExpressionType.HYBRID
            #     logger.info(f"Expression type determined: HYBRID ({len(parallel_tasks)} parallel tasks, mixed operations)")
        else:
            logger.info(
                f"Expression type determined: SEQUENTIAL (insufficient parallel opportunities, {len(parallel_tasks)} parallel tasks)")

        return parsed_expression

    def is_commutative_expression(self, tree: Union[ExpressionNode, float]) -> bool:
        if not isinstance(tree, ExpressionNode):
            return True

        if tree.operation not in [Operations.ADD]:
            return False

        left_commutative = self.is_commutative_expression(tree.left)
        right_commutative = self.is_commutative_expression(tree.right)

        return left_commutative and right_commutative

    def is_independent_subexpression(self, tree: ExpressionNode) -> bool:
        if not self.task_map:
            return False

        # Case: multiplication of a subexpression and a number
        if tree.operation == Operations.MUL:
            # Pattern: (subexpr) * number
            if (isinstance(tree.left, ExpressionNode) and isinstance(tree.right, (int, float))):
                return True
            # Pattern: number * (subexpr)
            if (isinstance(tree.left, (int, float)) and isinstance(tree.right, ExpressionNode)):
                return True

        # Case: division of a subexpression by a number
        elif tree.operation == Operations.DIV and isinstance(tree.left, ExpressionNode) and isinstance(tree.right,
                                                                                                       (int, float)):
            # Pattern: (subexpr) / number
            return True

        # Case: any simple operation that can be computed in one step
        elif tree.operation in self.task_map and isinstance(tree.left, (int, float)) and isinstance(tree.right,
                                                                                                    (int, float)):
            return True

        return False

    def extract_parallel_tasks_from_expression(self, tree: Union[ExpressionNode, float]) -> List:
        if not isinstance(tree, ExpressionNode) or not self.task_map:
            if isinstance(tree, (int, float)):
                add_task = self.task_map.get(Operations.ADD)
                if add_task:
                    return [add_task.s(tree, 0)]
            return []

        tasks = []

        # For commutative operations (add, sub), we can parallelize the operands
        if tree.operation in [Operations.ADD, Operations.SUB]:

            # Process left branch
            if isinstance(tree.left, ExpressionNode):
                # Case: Simple operation with two numbers
                if isinstance(tree.left.left, (int, float)) and isinstance(tree.left.right, (int, float)):
                    task_func = self.task_map.get(tree.left.operation)
                    if task_func:
                        tasks.append(task_func.s(tree.left.left, tree.left.right))
                # Case: Complex subexpression - recursively extract tasks
                else:
                    # If it's a complete subexpression (e.g., (1+2)*3), add it as a standalone task
                    if self.is_independent_subexpression(tree.left):
                        result = self.evaluate_subexpression_as_task(tree.left)
                        if result:
                            tasks.append(result)
                    else:
                        tasks.extend(self.extract_parallel_tasks_from_expression(tree.left))
            else:
                add_task = self.task_map.get(Operations.ADD)
                if add_task:
                    tasks.append(add_task.s(tree.left, 0))

            # Process right branch
            if isinstance(tree.right, ExpressionNode):
                # Case: Simple operation with two numbers
                if isinstance(tree.right.left, (int, float)) and isinstance(tree.right.right, (int, float)):
                    task_func = self.task_map.get(tree.right.operation)
                    if task_func:
                        tasks.append(task_func.s(tree.right.left, tree.right.right))
                # Case: Complex subexpression
                else:
                    # If it's a complete subexpression (e.g., 3*(1+2)), add it as a standalone task
                    if self.is_independent_subexpression(tree.right):
                        result = self.evaluate_subexpression_as_task(tree.right)
                        if result:
                            tasks.append(result)
                    else:
                        tasks.extend(self.extract_parallel_tasks_from_expression(tree.right))
            else:
                add_task = self.task_map.get(Operations.ADD)
                if add_task:
                    tasks.append(add_task.s(tree.right, 0))

        # For other operations (like multiply), check if branches can be parallelized
        elif tree.operation in [Operations.MUL, Operations.DIV]:
            # Check if each branch can be evaluated independently
            if self.is_independent_subexpression(tree):
                result = self.evaluate_subexpression_as_task(tree)
                if result:
                    tasks.append(result)
                    return tasks  # Return early as we're handling this entire subtree

        # If no tasks found so far, try to extract from children
        if not tasks:
            if isinstance(tree.left, ExpressionNode):
                tasks.extend(self.extract_parallel_tasks_from_expression(tree.left))
            if isinstance(tree.right, ExpressionNode):
                tasks.extend(self.extract_parallel_tasks_from_expression(tree.right))

        return tasks

    def evaluate_subexpression_as_task(self, tree: ExpressionNode):
        if not self.task_map:
            return None

        try:
            from celery import chain

            # Simple operation with two numbers
            if tree.operation in self.task_map and isinstance(tree.left, (int, float)) and isinstance(tree.right,
                                                                                                      (int, float)):
                return self.task_map[tree.operation].s(tree.left, tree.right)

            if tree.operation == Operations.MUL:
                # Pattern: (subexpr) * number
                if isinstance(tree.left, ExpressionNode) and isinstance(tree.right, (int, float)):
                    if tree.left.operation in self.task_map and isinstance(tree.left.left, (int, float)) and isinstance(
                            tree.left.right, (int, float)):
                        # First calculate inner expression
                        inner_task = self.task_map[tree.left.operation].s(tree.left.left, tree.left.right)
                        # Then multiply by the number
                        mul_task = self.task_map[Operations.MUL].s(tree.right)
                        # Chain them
                        return chain(inner_task, mul_task)

                # Pattern: number * (subexpr)
                if isinstance(tree.left, (int, float)) and isinstance(tree.right, ExpressionNode):
                    if tree.right.operation in self.task_map and isinstance(tree.right.left,
                                                                            (int, float)) and isinstance(
                            tree.right.right, (int, float)):
                        # First calculate inner expression
                        inner_task = self.task_map[tree.right.operation].s(tree.right.left, tree.right.right)
                        # Then multiply by the number
                        mul_task = self.task_map[Operations.MUL].s(tree.left)
                        # Chain them
                        return chain(inner_task, mul_task)

            # Pattern: (subexpr) / number
            elif tree.operation == Operations.DIV and isinstance(tree.left, ExpressionNode) and isinstance(tree.right,
                                                                                                           (int,
                                                                                                            float)):
                if tree.left.operation in self.task_map and isinstance(tree.left.left, (int, float)) and isinstance(
                        tree.left.right, (int, float)):
                    # First calculate inner expression
                    inner_task = self.task_map[tree.left.operation].s(tree.left.left, tree.left.right)
                    # Then divide by the number
                    div_task = self.task_map[Operations.DIV].s(tree.right)
                    # Chain them
                    return chain(inner_task, div_task)

            return None
        except Exception as e:
            logger.error(f"Error creating subtask for pattern: {str(e)}")
            return None

    def extract_final_operation(self, tree: Union[ExpressionNode, float]) -> Tuple[Optional[str], Optional[float]]:
        if not isinstance(tree, ExpressionNode):
            return None, None

        if tree.operation != Operations.ADD:
            if isinstance(tree.right, (int, float)):
                logger.info(f"Final operation extracted: {tree.operation} with operand {tree.right}")
                return tree.operation, float(tree.right)
            elif isinstance(tree.left, (int, float)):
                logger.info(f"Final operation extracted: {tree.operation} with operand {tree.left}")
                return tree.operation, float(tree.left)

        return None, None

    def flatten_expression_tree(self, tree: Union[ExpressionNode, float]) -> List[Dict[str, Any]]:
        if isinstance(tree, (int, float)):
            return []

        if not isinstance(tree, ExpressionNode):
            return []

        operations = []

        if isinstance(tree.left, ExpressionNode):
            left_ops = self.flatten_expression_tree(tree.left)
            operations.extend(left_ops)

        if isinstance(tree.right, ExpressionNode):
            right_ops = self.flatten_expression_tree(tree.right)
            operations.extend(right_ops)

        if isinstance(tree.left, (int, float)) and isinstance(tree.right, (int, float)):
            operations.append({
                'operation': tree.operation,
                'operand1': tree.left,
                'operand2': tree.right
            })
        elif isinstance(tree.left, ExpressionNode) and isinstance(tree.right, (int, float)):
            operations.append({
                'operation': tree.operation,
                'operand1': 'previous_result',
                'operand2': tree.right
            })
        elif isinstance(tree.left, (int, float)) and isinstance(tree.right, ExpressionNode):
            operations.append({
                'operation': tree.operation,
                'operand1': tree.left,
                'operand2': 'previous_result'
            })
        else:
            operations.append({
                'operation': tree.operation,
                'operand1': 'previous_result',
                'operand2': 'previous_result'
            })

        logger.info(f"Expression flattened to {len(operations)} sequential operations")
        return operations