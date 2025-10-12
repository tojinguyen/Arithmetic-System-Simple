from ..celery import app
from .add_service import add
from .sub_service import subtract
from .mul_service import multiply
from .div_service import divide
from .expression_parser import OperationEnum

OPERATION_MAP = {
    OperationEnum.ADD.value: add,
    OperationEnum.SUB.value: subtract,
    OperationEnum.MUL.value: multiply,
    OperationEnum.DIV.value: divide,
}

@app.task(name='combine_and_operate', queue='combine_tasks')
def combine_and_operate(results, operation_name, fixed_operand=None, is_left_fixed=False):
    op_func = OPERATION_MAP[operation_name]

    if isinstance(results, list):
        if len(results) != 2:
            raise ValueError(f"Combiner for list expects 2 elements, got {len(results)}")
        return op_func(results[0], results[1])
    
    elif fixed_operand is not None:
        if is_left_fixed:
            return op_func(fixed_operand, results)
        else:
            return op_func(results, fixed_operand)
    else:
        raise ValueError("Invalid call to combine_and_operate")
    