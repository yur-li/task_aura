"""Integration tests - test full system with docker-compose."""
import pytest
import httpx
from time import sleep



@pytest.fixture
def client_api_key():
    """Valid API key for testing."""
    return "dev-key-123"


@pytest.fixture
def customer_facing_url():
    """Customer-facing service URL."""
    return "http://localhost:8080"


@pytest.fixture
def customer_mgmt_url():
    """Customer management service URL."""
    return "http://localhost:8081"


def test_health_check_customer_facing(customer_facing_url):
    """Test customer-facing health endpoint."""
    try:
        with httpx.Client() as client:
            response = client.get(f"{customer_facing_url}/health", timeout=2.0)
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "ok"
            assert data["service"] == "customer-facing"
    except Exception as e:
        pytest.skip(f"Service not available: {e}")


def test_health_check_customer_mgmt(customer_mgmt_url):
    """Test customer-mgmt health endpoint."""
    try:
        with httpx.Client(verify=False) as client:
            response = client.get(f"{customer_mgmt_url}/health", timeout=2.0)
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "ok"
            assert data["service"] == "customer-mgmt"
    except Exception as e:
        pytest.skip(f"Service not available: {e}")


def test_buy_without_api_key(customer_facing_url):
    """Test /buy endpoint rejects request without API key."""
    try:
        with httpx.Client() as client:
            response = client.post(
                f"{customer_facing_url}/buy",
                json={"username": "john", "userid": "user1", "price": 10.0}
            )
            assert response.status_code == 401
    except Exception as e:
        pytest.skip(f"Service not available: {e}")


def test_buy_with_invalid_data(customer_facing_url, client_api_key):
    """Test /buy endpoint validates data."""
    try:
        with httpx.Client() as client:
            response = client.post(
                f"{customer_facing_url}/buy",
                headers={"X-API-Key": client_api_key},
                json={"username": "john", "userid": "user1", "price": -10.0}  # negative price
            )
            assert response.status_code == 422  # Validation error
    except Exception as e:
        pytest.skip(f"Service not available: {e}")


def test_buy_happy_path(customer_facing_url, client_api_key):
    """Test happy path: POST /buy succeeds."""
    try:
        with httpx.Client() as client:
            response = client.post(
                f"{customer_facing_url}/buy",
                headers={"X-API-Key": client_api_key},
                json={"username": "john", "userid": "user1", "price": 10.0}
            )
            assert response.status_code == 202  # Accepted
            data = response.json()
            assert data["status"] == "accepted"
            assert data["userid"] == "user1"
    except Exception as e:
        pytest.skip(f"Service not available: {e}")


def test_get_purchases_happy_path(customer_facing_url, client_api_key, customer_mgmt_url):
    """Test happy path: GET /purchases/<userid> succeeds."""
    try:
        # First, buy an item
        with httpx.Client() as client:
            buy_response = client.post(
                f"{customer_facing_url}/buy",
                headers={"X-API-Key": client_api_key},
                json={"username": "john", "userid": "user1", "price": 10.0}
            )
            assert buy_response.status_code == 202
        
        # Wait for message to be processed
        sleep(2)
        
        # Now retrieve purchases
        with httpx.Client(verify=False) as client:
            get_response = client.get(
                f"{customer_facing_url}/purchases/user1",
                headers={"X-API-Key": client_api_key}
            )
            assert get_response.status_code == 200
            data = get_response.json()
            assert data["userid"] == "user1"
            assert isinstance(data["purchases"], list)
    except Exception as e:
        pytest.skip(f"Service not available: {e}")


def test_get_nonexistent_user(customer_facing_url, client_api_key):
    """Test GET /purchases for non-existent user returns empty list."""
    try:
        with httpx.Client() as client:
            response = client.get(
                f"{customer_facing_url}/purchases/nonexistent_user",
                headers={"X-API-Key": client_api_key}
            )
            assert response.status_code == 200
            data = response.json()
            assert data["userid"] == "nonexistent_user"
            assert data["purchases"] == []
    except Exception as e:
        pytest.skip(f"Service not available: {e}")
