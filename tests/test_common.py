"""Unit tests for common utilities."""
import pytest
from app.common.errors import (
    ValidationError, AuthenticationError,
    NotFoundError, DatabaseError, KafkaError
)


def test_validation_error():
    """Test ValidationError has correct status code."""
    exc = ValidationError("Invalid input")
    assert exc.status_code == 400
    assert exc.error_code == "VALIDATION_ERROR"
    assert exc.message == "Invalid input"


def test_authentication_error():
    """Test AuthenticationError has correct status code."""
    exc = AuthenticationError("Invalid API key")
    assert exc.status_code == 401
    assert exc.error_code == "AUTH_ERROR"


def test_database_error():
    """Test DatabaseError formats message correctly."""
    exc = DatabaseError("Connection timeout")
    assert exc.status_code == 503
    assert exc.error_code == "DB_ERROR"
    assert "MongoDB" in exc.message


def test_kafka_error():
    """Test KafkaError formats message correctly."""
    exc = KafkaError("Broker unreachable")
    assert exc.status_code == 503
    assert exc.error_code == "KAFKA_ERROR"
    assert "Kafka" in exc.message


def test_not_found_error():
    """Test NotFoundError formats message."""
    exc = NotFoundError("User", "user123")
    assert exc.status_code == 404
    assert exc.error_code == "NOT_FOUND"
    assert "user123" in exc.message
