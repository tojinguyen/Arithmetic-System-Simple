from celery import chain, chord
from celery.result import AsyncResult
from typing import List, Optional
import logging

from .expression_parser import ParsedExpression, ExpressionNode, Operations
from .xsum_service import xsum as array_sum_task

logger = logging.getLogger(__name__)


class TaskExecutor:
    def __init__(self, task_map):
        self.task_map = task_map

    def execute_simple(self, parsed: ParsedExpression) -> AsyncResult:
        if not parsed.expression_tree:
            raise ValueError("No expression tree found in simple expression")

        tree = parsed.expression_tree
        if isinstance(tree, (int, float)):
            raise ValueError("Cannot execute a simple number")

        if not (isinstance(tree.left, (int, float)) and isinstance(tree.right, (int, float))):
            raise ValueError("Simple expression should have numeric operands")

        task_func = self.task_map.get(tree.operation)
        if not task_func:
            raise ValueError(f"Unsupported operation: {tree.operation}")

        # Using signature and apply_async instead of delay
        sig = task_func.signature((tree.left, tree.right))
        result = sig.apply_async()
        logger.info(f"Simple task created: {tree.operation}({tree.left}, {tree.right}) (ID: {result.id})")
        return result

    def execute_sequential(self, parsed: ParsedExpression) -> AsyncResult:
        if not parsed.expression_tree:
            return self.execute_simple(parsed)

        # Build the chain signature, then execute it
        workflow = self._build_task_chain(parsed.expression_tree)
        result = workflow.apply_async()
        logger.info(f"Sequential workflow created (direct tree): (ID: {result.id})")
        return result

    def _build_task_chain(self, node):
        if isinstance(node, (int, float)):
            raise ValueError("Cannot create task from single number")

        if not hasattr(node, 'operation') or not hasattr(node, 'left') or not hasattr(node, 'right'):
            raise ValueError("Invalid expression node")

        task_func = self.task_map.get(node.operation)
        if not task_func:
            raise ValueError(f"Unsupported operation: {node.operation}")

        # Both operands are numbers - return signature, not AsyncResult
        if isinstance(node.left, (int, float)) and isinstance(node.right, (int, float)):
            return task_func.signature((node.left, node.right))

        # Left is an expression, right is a number
        if not isinstance(node.left, (int, float)) and isinstance(node.right, (int, float)):
            left_task_chain = self._build_task_chain(node.left)
            return chain(left_task_chain, task_func.s(node.right))

        # Left is a number, right is an expression
        if isinstance(node.left, (int, float)) and not isinstance(node.right, (int, float)):
            right_task_chain = self._build_task_chain(node.right)
            if node.operation in Operations.COMMUTATIVE:
                return chain(right_task_chain, task_func.s(node.left))
            else:
                return chain(
                    right_task_chain,
                    task_func.s(node.left).set(kwargs={'swap': True})
                )

        left_task_chain = self._build_task_chain(node.left)
        right_task_chain = self._build_task_chain(node.right)

        return chain(left_task_chain, right_task_chain, task_func.s())

    def execute_parallel(self, parsed: ParsedExpression, parallel_tasks: List) -> AsyncResult:
        workflow = chord(parallel_tasks, array_sum_task.s())
        result = workflow.apply_async()
        logger.info(f"Parallel workflow created: {len(parallel_tasks)} tasks (ID: {result.id})")
        return result

    def execute_hybrid(self, parsed: ParsedExpression, parallel_tasks: List, final_operation: Optional[str] = None,
                       final_operand: Optional[float] = None) -> AsyncResult:
        if len(parallel_tasks) >= 2:
            if final_operation and final_operand is not None:
                parallel_chord = chord(parallel_tasks, array_sum_task.s())
                final_task = self.task_map[final_operation]
                workflow = chain(parallel_chord, final_task.s(final_operand))
                result = workflow.apply_async()
                logger.info(
                    f"Hybrid workflow created: {len(parallel_tasks)} parallel tasks → {final_operation} {final_operand} (ID: {result.id})")
                return result
            else:
                workflow = chord(parallel_tasks, array_sum_task.s())
                result = workflow.apply_async()
                logger.info(f"Parallel sum workflow created: {len(parallel_tasks)} tasks (ID: {result.id})")
                return result

        logger.info("Hybrid execution fallback to sequential")
        return self.execute_sequential(parsed)