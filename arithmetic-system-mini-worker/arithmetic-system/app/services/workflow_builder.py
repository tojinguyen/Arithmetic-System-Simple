from typing import Union, List
from .expression_parser import ExpressionNode
import logging
from mini.worker.workers.canvas import Node, Chain, Chord
from ..models.worker_models import CalculatorInput, AggregatorInput
from ..constants.constants import (
    OperationEnum,
    OPERATION_TOPIC_MAP,
    AGGREGATOR_TOPIC_MAP
)

logger = logging.getLogger(__name__)


class WorkflowBuilder:

    def build(self, expression_tree: Union[ExpressionNode, float]) -> Union[Node, Chain, Chord, float]:
        logger.info(f"Building {expression_tree}")
        return self._build_recursive(expression_tree)

    def _build_recursive(self, node: Union[ExpressionNode, float]) -> Union[Node, Chain, Chord, float]:
        if isinstance(node, (int, float)):
            return float(node)

        if not isinstance(node, ExpressionNode):
            raise TypeError(f"Invalid node type: {type(node)}")

        logger.info(f"Building recursive node: {node}")

        is_left_constant = isinstance(node.left, (int, float))
        is_right_constant = isinstance(node.right, (int, float))
        if is_left_constant and is_right_constant:
            task_input = CalculatorInput(x=node.left, y=node.right)
            op_node = Node(
                topic=OPERATION_TOPIC_MAP[node.operation],
                input=task_input.model_dump_json()
            )
            return Chain(nodes=[op_node])

        if node.operation.is_commutative and not is_left_constant and not is_right_constant:
            return self._build_flat_workflow(node)
        else:
            left_workflow = self._build_recursive(node.left)
            right_workflow = self._build_recursive(node.right)

            is_left_task = isinstance(left_workflow, Node)
            is_right_task = isinstance(right_workflow, Node)

            # If Left is task and Right is constant
            if is_left_task and not is_right_task:
                op_input = CalculatorInput(y=right_workflow, is_left_fixed=False)
                op_node = Node(
                    topic=OPERATION_TOPIC_MAP[node.operation],
                    input=op_input.model_dump_json()
                )
                return Chain(nodes=[left_workflow, op_node])

            # If Left is constant and Right is task
            elif not is_left_task and is_right_task:
                op_input = CalculatorInput(y=left_workflow, is_left_fixed=True)
                op_node = Node(
                    topic=OPERATION_TOPIC_MAP[node.operation],
                    input=op_input.model_dump_json()
                )
                return Chain(nodes=[right_workflow, op_node])

            # If both are tasks, create a Chord to run them in parallel followed by Combiner
            else:
                callback_node = Node(topic=OPERATION_TOPIC_MAP[node.operation])
                return Chord(
                    nodes=[left_workflow, right_workflow],
                    callback=callback_node
                )

    def _collect_operands(self, node, operation: OperationEnum):
        operands = []
        if isinstance(node, ExpressionNode) and node.operation == operation:
            operands.extend(self._collect_operands(node.left, operation))
            operands.extend(self._collect_operands(node.right, operation))
        else:
            operands.append(node)
        return operands

    def _build_flat_workflow(self, node: ExpressionNode) -> Union[Node, Chord]:
        all_operands = self._collect_operands(node, node.operation)
        child_workflows = [self._build_recursive(op) for op in all_operands]

        tasks: List[Union[Node, Chain, Chord]] = []
        constants: List[float] = []

        for wf in child_workflows:
            if isinstance(wf, float):
                constants.append(wf)
            else:
                tasks.append(wf)

        aggregator_topic = AGGREGATOR_TOPIC_MAP.get(node.operation)
        if not aggregator_topic:
            raise ValueError(f"No aggregator for commutative operation: {node.operation}")

        aggregator_topic = AGGREGATOR_TOPIC_MAP[node.operation]
        aggregator_input = AggregatorInput(constants=constants)

        if not tasks:
            return Node(
                topic=aggregator_topic,
                input=aggregator_input.model_dump_json()
            )

        return Chord(
            nodes=tasks,
            callback=Node(
                topic=aggregator_topic,
                input=aggregator_input.model_dump_json()
            )
        )
