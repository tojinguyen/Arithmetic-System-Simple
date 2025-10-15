from enum import Enum

class OperationEnum(str, Enum):
    ADD = "add"
    SUB = "sub"
    MUL = "mul"
    DIV = "div"

    @property
    def is_commutative(self):
        return self in {OperationEnum.ADD, OperationEnum.MUL}

ADD_TASKS_TOPIC = "add_tasks"
SUB_TASKS_TOPIC = "sub_tasks"
MUL_TASKS_TOPIC = "mul_tasks"
DIV_TASKS_TOPIC = "div_tasks"

XSUM_TASKS_TOPIC = "xsum_tasks"
XPROD_TASKS_TOPIC = "xprod_tasks"

OPERATION_TOPIC_MAP = {
    OperationEnum.ADD: ADD_TASKS_TOPIC,
    OperationEnum.SUB: SUB_TASKS_TOPIC,
    OperationEnum.MUL: MUL_TASKS_TOPIC,
    OperationEnum.DIV: DIV_TASKS_TOPIC,
}

AGGREGATOR_TOPIC_MAP = {
    OperationEnum.ADD: XSUM_TASKS_TOPIC,
    OperationEnum.MUL: XPROD_TASKS_TOPIC,
}