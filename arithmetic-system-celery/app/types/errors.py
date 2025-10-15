class ExpressionError(Exception): ...


class UnsupportedNodeError(ExpressionError):
    def __init__(self, node_type: str):
        self.node_type = node_type
        super().__init__(f"Unsupported node type: {node_type}")


class UnsupportedUnaryOperatorError(ExpressionError):
    def __init__(self, operator_type: str):
        self.operator_type = operator_type
        super().__init__(f"Unsupported unary operator: {operator_type}")


class ComplexUnaryExpressionError(ExpressionError):
    def __init__(self, expression: str):
        self.expression = expression
        super().__init__(
            f"Unary subtraction on complex expression is not supported: '{expression}'"
        )


class ExpressionSyntaxError(ExpressionError):
    def __init__(self, expression: str, message: str = "Invalid arithmetic expression"):
        self.expression = expression
        self.message = message
        super().__init__(f"{message}: '{expression}'")


class UnsupportedOperatorError(ExpressionError):
    def __init__(
        self, operator: str, message: str = "Unsupported operator in expression"
    ):
        self.operator = operator
        self.message = message
        super().__init__(f"{message}: '{operator}'")
