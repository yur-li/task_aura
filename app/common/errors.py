"""
Custom exceptions for Task Aura services.
Allows consistent error handling and response codes across all services.
"""


class TaskAuraException(Exception):
    """Base exception for all Task Aura errors."""

    def __init__(
        self, message: str, status_code: int = 500, error_code: str = "INTERNAL_ERROR"
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_code = error_code


class ValidationError(TaskAuraException):
    """Raised when request data validation fails."""

    def __init__(self, message: str, error_code: str = "VALIDATION_ERROR"):
        super().__init__(message, status_code=400, error_code=error_code)


class AuthenticationError(TaskAuraException):
    """Raised when authentication fails (missing/invalid API key)."""

    def __init__(
        self, message: str = "Authentication failed", error_code: str = "AUTH_ERROR"
    ):
        super().__init__(message, status_code=401, error_code=error_code)


class NotFoundError(TaskAuraException):
    """Raised when requested resource is not found."""

    def __init__(self, resource: str, identifier: str):
        message = f"{resource} not found: {identifier}"
        super().__init__(message, status_code=404, error_code="NOT_FOUND")


class ExternalServiceError(TaskAuraException):
    """Raised when external service (Kafka, MongoDB) fails."""

    def __init__(
        self, service: str, message: str, error_code: str = "SERVICE_UNAVAILABLE"
    ):
        full_message = f"{service} error: {message}"
        super().__init__(full_message, status_code=503, error_code=error_code)


class DatabaseError(ExternalServiceError):
    """Raised when MongoDB operation fails."""

    def __init__(self, message: str):
        super().__init__("MongoDB", message, error_code="DB_ERROR")


class KafkaError(ExternalServiceError):
    """Raised when Kafka operation fails."""

    def __init__(self, message: str):
        super().__init__("Kafka", message, error_code="KAFKA_ERROR")
