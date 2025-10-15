import pytest
from fastapi.testclient import TestClient

# pytest.fixture `client` đã được định nghĩa trong conftest.py
# nên chúng ta có thể sử dụng nó trực tiếp trong các hàm test.


class TestCalculateAPI:
    """Test suite for the /api/calculate endpoint."""

    @pytest.mark.parametrize(
        "expression, expected_result",
        [
            # --- Basic Operations ---
            ("5 + 3", 8.0),
            ("10 - 4", 6.0),
            ("6 * 7", 42.0),
            ("20 / 5", 4.0),
            # --- Floating Point and Negative Numbers ---
            ("-5 + 3", -2.0),
            ("10 * -2", -20.0),
            ("2.5 * 4", 10.0),
            ("10 / 4", 2.5),
            ("-10 / -2", 5.0),
            # --- Operator Precedence ---
            ("2 + 3 * 4", 14.0),
            ("10 - 8 / 2", 6.0),
            ("5 * 2 + 10 / 2", 15.0),
            # --- Parentheses ---
            ("(2 + 3) * 4", 20.0),
            ("(10 - 8) / 2", 1.0),
            ("10 * (6 - (2 + 2))", 20.0),
            # --- Complex/Chained Operations ---
            ("1+2+3+4+5", 15.0),
            ("(1+2)*(3+4)", 21.0),
            ("100 / (2 * (10 + 15))", 2.0),
        ],
    )
    def test_calculate_valid_expressions(
        self, client: TestClient, expression: str, expected_result: float
    ):
        """Tests that valid arithmetic expressions are evaluated correctly."""
        response = client.get("/api/calculate", params={"expression": expression})

        assert response.status_code == 200, (
            f"Failed on expression: '{expression}'. Response: {response.text}"
        )
        data = response.json()
        assert "result" in data
        assert data["result"] == pytest.approx(expected_result)

    @pytest.mark.parametrize(
        "expression, status_code, error_message_part",
        [
            # --- Syntax Errors (from ExpressionParser) ---
            ("5+*3", 400, "Syntax error"),
            ("5 + ", 400, "Syntax error"),
            ("(5 + 3", 400, "Syntax error"),
            ("5 + 3)", 400, "Syntax error"),
            # --- Runtime Errors (from Celery workers) ---
            ("10 / 0", 400, "Cannot divide by zero"),
            ("(5 - 5) / (2 - 2)", 400, "Cannot divide by zero"),
            # --- Validation Errors (from ExpressionParser) ---
            ("5 + a", 400, "Expression contains invalid characters"),
            ("ten / two", 400, "Expression contains invalid characters"),
            ("5 % 2", 400, "Unsupported operator"),
            # --- FastAPI Validation Errors ---
            ("", 400, "Expression cannot be empty"),  # Empty string is passed to parser
        ],
    )
    def test_calculate_invalid_expressions(
        self,
        client: TestClient,
        expression: str,
        status_code: int,
        error_message_part: str,
    ):
        response = client.get("/api/calculate", params={"expression": expression})

        assert response.status_code == status_code, (
            f"Expected {status_code} on '{expression}', but got {response.status_code}"
        )
        data = response.json()
        assert "detail" in data
        assert error_message_part in data["detail"]

    def test_calculate_with_extra_whitespace(self, client: TestClient):
        expression = "  ( 10 +  5 )   * 2  "
        response = client.get("/api/calculate", params={"expression": expression})

        assert response.status_code == 200
        data = response.json()
        assert data["result"] == 30.0

    def test_calculate_no_expression_provided(self, client: TestClient):
        response = client.get("/api/calculate")

        assert response.status_code == 422
        data = response.json()
        assert "Field required" in str(data["detail"])
