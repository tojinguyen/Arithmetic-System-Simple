from ..celery import app
from .add_service import add
from .sub_service import subtract
from .mul_service import multiply
from .div_service import divide
from .expression_parser import Operations

OPERATION_MAP = {
    Operations.ADD: add,
    Operations.SUB: subtract,
    Operations.MUL: multiply,
    Operations.DIV: divide,
}

@app.task(name='combine_and_operate', queue='mul_tasks')
def combine_and_operate(results, operation_name, fixed_operand=None, is_left_fixed=False):
    """
    Task này nhận kết quả và thực hiện phép toán cuối cùng.
    - results: Có thể là một list (từ group) hoặc một số (từ chain).
    - operation_name: Tên của phép toán ('add', 'sub', ...).
    - fixed_operand: Một số cố định (dùng cho trường hợp num + task).
    - is_left_fixed: True nếu số cố định là toán hạng bên trái (ví dụ: 5 - result).
    """
    op_func = OPERATION_MAP[operation_name]

    if isinstance(results, list): # Trường hợp (task + task)
        # results từ group sẽ là [result_left, result_right]
        return op_func(results[0], results[1])
    
    elif fixed_operand is not None: # Trường hợp (num + task) hoặc (task + num)
        if is_left_fixed:
            # ví dụ: 5 - (1+2), fixed_operand=5, results là kết quả của (1+2)
            return op_func(fixed_operand, results)
        else:
            # ví dụ: (1+2) - 5, fixed_operand=5, results là kết quả của (1+2)
            # Logic mặc định của chain đã xử lý op_task.s(operand)
            # Tuy nhiên để tổng quát, chúng ta có thể làm thế này
            return op_func(results, fixed_operand)
    else: # Trường hợp (task + num) thông thường
        # Trường hợp này được xử lý bởi `chain(task, op_task.s(num))`
        # Nhưng chúng ta vẫn cần xử lý để hàm này hoàn chỉnh
        # Trong thực tế, logic của _build_recursive sẽ không gọi tới nhánh này
        raise ValueError("Lỗi logic: combine_and_operate được gọi không hợp lệ")

# Đừng quên thêm 'app.services.combiner_service' vào include list trong app/celery.py