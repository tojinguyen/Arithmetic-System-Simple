from celery import chain, group
from app.services.expression_parser import ExpressionParser
from app.services.workflow_builder import WorkflowBuilder
from app.services.add_service import add
from app.services.mul_service import multiply

def test_build_simple_workflow():
    """Test building workflow for simple expression: 2 * 3"""
    parser = ExpressionParser()
    task_map = {'mul': multiply}
    builder = WorkflowBuilder(task_map)

    parsed_expr = parser.parse("2 * 3")
    workflow = builder._build_recursive(parsed_expr.expression_tree)
    
    assert workflow.task == 'multiply'
    assert workflow.args == (2.0, 3.0)

def test_build_chain_workflow():
    """Test building workflow for a chain: (2 + 3) * 4"""
    parser = ExpressionParser()
    task_map = {'add': add, 'mul': multiply}
    builder = WorkflowBuilder(task_map)

    parsed_expr = parser.parse("(2 + 3) * 4")
    workflow = builder._build_recursive(parsed_expr.expression_tree)

    assert isinstance(workflow, chain)
    
    first_task = workflow.tasks[0]
    assert first_task.task == 'add'
    assert first_task.args == (2.0, 3.0)

    second_task = workflow.tasks[1]
    assert second_task.task == 'combine_and_operate'
    assert second_task.kwargs['operation_name'] == 'mul'
    assert second_task.kwargs['fixed_operand'] == 4.0