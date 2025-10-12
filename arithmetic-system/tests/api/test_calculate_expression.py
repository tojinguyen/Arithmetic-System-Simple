def test_calculate_simple_expression(client):
    """Test endpoint /calculate with simple expression."""
    response = client.get("/api/calculate?expression=5%2B3")
    assert response.status_code == 200
    data = response.json()
    assert data["result"] == 8.0
    assert data["original_expression"] == "5+3"

def test_calculate_complex_expression(client):
    """Test endpoint /calculate with complex expression."""
    expression = "(4 + 8) * (10 - 5) / 2"
    response = client.get(f"/api/calculate?expression={expression}")
    assert response.status_code == 200
    data = response.json()
    assert data["result"] == 30.0
    assert data["original_expression"] == expression

def test_calculate_invalid_expression(client):
    """Test endpoint /calculate with invalid expression."""
    response = client.get("/api/calculate?expression=5+*3")
    assert response.status_code == 400
    assert "detail" in response.json()
    assert "Invalid expression" in response.json()["detail"]

def test_calculate_division_by_zero(client):
    """Test endpoint /calculate with division by zero."""
    response = client.get("/api/calculate?expression=10/0")
    assert response.status_code == 400
    assert "detail" in response.json()
    assert "Cannot divide by zero" in response.json()["detail"]