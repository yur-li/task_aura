"""Unit tests for Customer Management Service."""
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime


def test_purchase_database_schema():
    """Test PurchaseDatabase is properly configured."""
    from app.customer_mgmt.db import PurchaseDatabase
    
    mock_db = MagicMock()
    db = PurchaseDatabase(mock_db, "purchases")
    
    assert db.collection_name == "purchases"
    assert db.db == mock_db


def test_purchases_list_response_schema():
    """Test purchases list response schema."""
    from app.customer_mgmt.main import PurchasesListResponse
    
    response = PurchasesListResponse(
        userid="user1",
        purchases=[],
        total_count=0,
        limit=100,
        skip=0,
        has_more=False
    )
    
    assert response.userid == "user1"
    assert response.total_count == 0
    assert response.has_more is False
    assert len(response.purchases) == 0


@pytest.mark.asyncio
async def test_kafka_consumer_initialization():
    """Test KafkaConsumer can be initialized."""
    from app.customer_mgmt.kafka_consumer import PurchaseConsumer
    
    mock_db = MagicMock()
    mock_collection = AsyncMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)
    
    consumer = PurchaseConsumer(
        bootstrap_servers=["kafka:9092"],
        topic="purchases",
        db=mock_db,
        batch_size=100
    )
    
    assert consumer.topic == "purchases"
    assert consumer.batch_size == 100
    assert consumer.message_batch == []
    assert consumer.running is False


def test_health_response_schema():
    """Test health response schema."""
    from app.customer_mgmt.main import HealthResponse
    
    response = HealthResponse(
        status="ok",
        service="customer-mgmt",
        timestamp=datetime.utcnow().isoformat()
    )
    
    assert response.status == "ok"
    assert response.service == "customer-mgmt"
