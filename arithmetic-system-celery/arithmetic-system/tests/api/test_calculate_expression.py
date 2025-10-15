import pytest
from fastapi.testclient import TestClient

@pytest.mark.parametrize("expression, expected_result", [
    ("5 + 3", 8.0),
    ("10 - 4", 6.0),
    ("6 * 7", 42.0),
    ("20 / 5", 4.0),

    ("-5 + 3", -2.0),
    ("10 * -2", -20.0),
    ("2.5 * 4", 10.0),
    ("10 / 4", 2.5),
    ("-10 / -2", 5.0),

    ("2 + 3 * 4", 14.0),
    ("10 - 8 / 2", 6.0),
    ("5 * 2 + 10 / 2", 15.0),

    ("(2 + 3) * 4", 20.0),
    ("(10 - 8) / 2", 1.0),
    ("10 * (6 - (2 + 2))", 20.0),

    ("1+2+3+4+5", 15.0), 
    ("(1+2)*(3+4)", 21.0), 
    ("100 / (2 * (10 + 15))", 2.0),
])
def test_calculate_valid_expressions(client: TestClient, expression: str, expected_result: float):
    response = client.get("/api/calculate", params={"expression": expression})
    
    assert response.status_code == 200, f"Failed on expression: '{expression}'. Response: {response.text}"
    
    data = response.json()
    assert data["original_expression"] == expression
    assert data["result"] == pytest.approx(expected_result)


@pytest.mark.parametrize("expression, error_message_part", [
    ("5+*3", "Invalid expression"),
    ("5 +", "Invalid expression"),
    ("(5 + 3", "Invalid expression"),
    ("5 + 3)", "Invalid expression"),
    
    ("10 / 0", "Cannot divide by zero"),
    ("(5 - 5) / (2 - 2)", "Cannot divide by zero"),

    ("5 + a", "invalid characters"),
    ("ten / two", "invalid characters"),
    ("5 % 2", "Unsupported operator"),
])
def test_calculate_invalid_expressions(client: TestClient, expression: str, error_message_part: str):
    response = client.get("/api/calculate", params={"expression": expression})
    
    assert response.status_code == 400, f"Expected 400 on expression: '{expression}', but got {response.status_code}"
    
    data = response.json()
    assert "detail" in data
    assert error_message_part in data["detail"]

def test_calculate_with_extra_whitespace(client: TestClient):
    """
    Test that the parser correctly handles extra whitespace in the expression.
    """
    expression = "  ( 10 +  5 )   * 2  "
    response = client.get("/api/calculate", params={"expression": expression})
    
    assert response.status_code == 200
    data = response.json()
    assert data["result"] == 30.0
    assert data["original_expression"] == expression

def test_calculate_no_expression_provided(client: TestClient):
    response = client.get("/api/calculate")
    assert response.status_code == 422
    data = response.json()
    assert "Field required" in str(data['detail'])