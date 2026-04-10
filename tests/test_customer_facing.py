"""Unit tests for Customer-Facing Service."""
import pytest
from unittest.mock import AsyncMock, MagicMock
from fastapi.testclient import TestClient


@pytest.fixture
def mock_kafka():
    """Mock Kafka producer."""
    producer = AsyncMock()
    producer.send_and_wait = AsyncMock(return_value=MagicMock(offset=0, partition=0))
    return producer


@pytest.fixture  
def mock_http_client():
    """Mock HTTP client."""
    client = AsyncMock()
    response = AsyncMock()
    response.status_code = 200
    response.json.return_value = {
        "userid": "user1",
        "purchases": [
            {"price": 10.0, "timestamp": "2024-01-01T00:00:00"}
        ]
    }
    client.get = AsyncMock(return_value=response)
    return client


def test_purchase_request_schema_validation():
    """Test purchase request data model validates fields."""
    from app.customer_facing.main import PurchaseRequest
    
    # Valid request
    request = PurchaseRequest(username="john", userid="user1", price=10.0)
    assert request.username == "john"
    assert request.userid == "user1"
    assert request.price == 10.0
    
    # Invalid: negative price
    with pytest.raises(ValueError):
        PurchaseRequest(username="john", userid="user1", price=-5.0)
    
    # Invalid: zero price
    with pytest.raises(ValueError):
        PurchaseRequest(username="john", userid="user1", price=0)


def test_health_endpoint_schema():
    """Test health endpoint response schema."""
    from app.customer_facing.main import HealthResponse
    from datetime import datetime
    
    response = HealthResponse(
        status="ok",
        service="customer-facing",
        timestamp=datetime.utcnow().isoformat()
    )
    
    assert response.status == "ok"
    assert response.service == "customer-facing"
    assert isinstance(response.timestamp, str)


def test_purchase_message_schema():
    """Test purchase message schema."""
    from app.customer_facing.main import PurchaseMessage
    
    msg = PurchaseMessage(
        username="john",
        userid="user1",
        price=10.0,
        timestamp="2024-01-01T00:00:00Z"
    )
    
    assert msg.username == "john"
    assert msg.userid == "user1"
    assert msg.price == 10.0


def test_cors_preflight_for_buy_endpoint():
    """Browser preflight for the UI should be accepted."""
    from app.customer_facing.main import app

    client = TestClient(app)
    response = client.options(
        "/buy",
        headers={
            "Origin": "http://localhost:3001",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type,x-api-key",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:3001"
    assert "POST" in response.headers["access-control-allow-methods"]
