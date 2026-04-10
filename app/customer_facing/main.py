"""
Customer-Facing Web Service for Task Aura
Handles HTTP requests for:
1. POST /buy - Accept purchase requests, publish to Kafka
2. GET /purchases/{userid} - Retrieve purchases from Customer Management API
"""

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
import os
import json
import time
from datetime import datetime
from typing import Optional
import httpx

from aiokafka import AIOKafkaProducer
from pydantic import BaseModel, Field, field_validator

from app.common.logging_setup import setup_logging
from app.common.metrics import init_metrics, get_metrics, start_metrics_server
from app.common.defaults import SERVICE_DEFAULTS, CUSTOMER_FACING_DEFAULTS
from app.common.errors import (
    ValidationError,
    AuthenticationError,
    ExternalServiceError,
    KafkaError,
    TaskAuraException,
)

# ============================================================================
# Configuration & Setup
# ============================================================================

# Initialize logging
logger = setup_logging(
    "customer-facing", level=os.getenv("LOG_LEVEL", SERVICE_DEFAULTS["LOG_LEVEL"])
)

# Initialize metrics
init_metrics("customer-facing")
metrics = get_metrics()

# Load configuration from environment
API_PORT = int(os.getenv("API_PORT", CUSTOMER_FACING_DEFAULTS["API_PORT"]))
METRICS_PORT = int(
    os.getenv("METRICS_PORT", CUSTOMER_FACING_DEFAULTS["METRICS_PORT"])
)
KAFKA_BROKERS = os.getenv(
    "KAFKA_BROKERS", SERVICE_DEFAULTS["KAFKA_BROKERS"]
).split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", SERVICE_DEFAULTS["KAFKA_TOPIC"])
KAFKA_SASL_MECHANISM = os.getenv(
    "KAFKA_SASL_MECHANISM", SERVICE_DEFAULTS["KAFKA_SASL_MECHANISM"]
)
KAFKA_SASL_PLAIN_USERNAME = os.getenv(
    "KAFKA_SASL_PLAIN_USERNAME", CUSTOMER_FACING_DEFAULTS["KAFKA_SASL_PLAIN_USERNAME"]
)
KAFKA_SASL_PLAIN_PASSWORD = os.getenv(
    "KAFKA_SASL_PLAIN_PASSWORD", SERVICE_DEFAULTS["KAFKA_SASL_PLAIN_PASSWORD"]
)
KAFKA_SECURITY_PROTOCOL = os.getenv(
    "KAFKA_SECURITY_PROTOCOL", SERVICE_DEFAULTS["KAFKA_SECURITY_PROTOCOL"]
)

CUSTOMER_MGMT_URL = os.getenv(
    "CUSTOMER_MGMT_URL", CUSTOMER_FACING_DEFAULTS["CUSTOMER_MGMT_URL"]
)
REQUEST_TIMEOUT = float(
    os.getenv("REQUEST_TIMEOUT", CUSTOMER_FACING_DEFAULTS["REQUEST_TIMEOUT"])
)

# Valid API keys (in production, load from secure store)
VALID_API_KEYS = set(
    os.getenv("VALID_API_KEYS", CUSTOMER_FACING_DEFAULTS["VALID_API_KEYS"]).split(",")
)
CORS_ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv(
        "CORS_ALLOWED_ORIGINS",
        CUSTOMER_FACING_DEFAULTS["CORS_ALLOWED_ORIGINS"],
    ).split(",")
    if origin.strip()
]

logger.info(f"Starting Customer-Facing Service on port {API_PORT}")
logger.info(f"Kafka brokers: {KAFKA_BROKERS}")
logger.info(f"Customer Management URL: {CUSTOMER_MGMT_URL}")

# ============================================================================
# Data Models
# ============================================================================


class PurchaseRequest(BaseModel):
    """Schema for purchase requests."""

    username: str = Field(..., min_length=1, max_length=255)
    userid: str = Field(..., min_length=1, max_length=255)
    price: float = Field(..., gt=0, le=1000000)

    @field_validator("price")
    @classmethod
    def validate_price(cls, v):
        """Validate price is reasonable (not NaN, not infinite)."""
        if not isinstance(v, (int, float)):
            raise ValueError("price must be a number")
        if v <= 0:
            raise ValueError("price must be positive")
        return round(v, 2)


class PurchaseMessage(BaseModel):
    """Message structure for Kafka."""

    username: str
    userid: str
    price: float
    timestamp: str


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    service: str
    timestamp: str


# ============================================================================
# FastAPI Application
# ============================================================================

app = FastAPI(
    title="Task Aura - Customer Facing Service",
    description="Handles customer purchase requests and queries",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _tracked_endpoint(request: Request) -> Optional[str]:
    route = request.scope.get("route")
    endpoint = getattr(route, "path", request.url.path)

    if endpoint in {"/health", "/ready", "/metrics"}:
        return None

    return endpoint


@app.middleware("http")
async def collect_http_metrics(request: Request, call_next):
    start_time = time.perf_counter()
    response = None
    status_code = 500

    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        endpoint = _tracked_endpoint(request)
        if not endpoint:
            pass
        else:
            request_size = request.headers.get("content-length")
            if request_size:
                metrics.http_request_size.labels(
                    method=request.method, endpoint=endpoint
                ).observe(float(request_size))

            status = str(status_code)
            duration = time.perf_counter() - start_time
            metrics.http_request_total.labels(
                method=request.method, endpoint=endpoint, status=status
            ).inc()
            metrics.http_request_duration.labels(
                method=request.method, endpoint=endpoint, status=status
            ).observe(duration)

            if response is not None:
                response_size = response.headers.get("content-length")
                if response_size:
                    metrics.http_response_size.labels(
                        method=request.method, endpoint=endpoint
                    ).observe(float(response_size))

# Global Kafka producer (initialized at startup)
kafka_producer: Optional[AIOKafkaProducer] = None

# Global HTTP client for internal service communication
httpx_client: Optional[httpx.AsyncClient] = None


# ============================================================================
# Startup & Shutdown Events
# ============================================================================


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    global kafka_producer, httpx_client

    logger.info("Startup event: Initializing Kafka producer and HTTP client")

    # Start Prometheus metrics server
    try:
        start_metrics_server(METRICS_PORT, registry=metrics.registry)
        logger.info(f"Prometheus metrics server started on port {METRICS_PORT}")
        metrics.set_service_health(True)
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")

    # Initialize Kafka producer
    try:
        kafka_config = {
            "bootstrap_servers": KAFKA_BROKERS,
            "sasl_mechanism": KAFKA_SASL_MECHANISM,
            "sasl_plain_username": KAFKA_SASL_PLAIN_USERNAME,
            "sasl_plain_password": KAFKA_SASL_PLAIN_PASSWORD,
            "security_protocol": KAFKA_SECURITY_PROTOCOL,
        }

        kafka_producer = AIOKafkaProducer(
            **kafka_config,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",  # Wait for all replicas
        )
        await kafka_producer.start()
        logger.info("Kafka producer initialized successfully")
        metrics.set_dependency_health("kafka", True)
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {e}")
        metrics.set_dependency_health("kafka", False)
        raise

    # Initialize HTTP client for internal service communication
    try:
        httpx_client = httpx.AsyncClient(
            verify=False,  # Disable cert verification (mTLS removed)
            timeout=REQUEST_TIMEOUT,
        )
        logger.info("HTTP client initialized")
        metrics.set_dependency_health("customer-mgmt", True)
    except Exception as e:
        logger.error(f"Failed to initialize HTTP client: {e}")
        metrics.set_dependency_health("customer-mgmt", False)
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown."""
    global kafka_producer, httpx_client

    logger.info("Shutdown event: Cleaning up resources")

    # Close Kafka producer
    if kafka_producer:
        await kafka_producer.stop()
        logger.info("Kafka producer closed")

    # Close HTTP client
    if httpx_client:
        await httpx_client.aclose()
        logger.info("HTTP client closed")


# ============================================================================
# Middleware & Authentication
# ============================================================================


async def validate_api_key(x_api_key: Optional[str] = Header(None)) -> str:
    """
    Validate API key in request header.
    All endpoints except /health require valid API key.
    """
    if not x_api_key:
        logger.warning("Request missing X-API-Key header")
        raise AuthenticationError("Missing X-API-Key header")

    if x_api_key not in VALID_API_KEYS:
        logger.warning(f"Invalid API key attempted")
        raise AuthenticationError("Invalid API key")

    return x_api_key


# ============================================================================
# Health & Readiness Checks
# ============================================================================


@app.get("/health", tags=["Health"], response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    Liveness probe: Service is running.
    K8s will restart pod if this fails.
    """
    return HealthResponse(
        status="ok", service="customer-facing", timestamp=datetime.utcnow().isoformat()
    )


@app.get("/ready", tags=["Health"], response_model=HealthResponse)
async def readiness_check() -> HealthResponse:
    """
    Readiness probe: Service is ready to accept traffic.
    K8s will remove pod from load balancer if this fails.
    Checks: Kafka connectivity, Customer Mgmt API connectivity
    """
    try:
        # Check Kafka
        if not kafka_producer:
            raise ExternalServiceError("Kafka", "Producer not initialized")

        # Check Customer Management API
        if not httpx_client:
            raise ExternalServiceError("CustomerMgmt", "HTTP client not initialized")

        # Quick connectivity check to Customer Mgmt API
        try:
            response = await httpx_client.get(
                f"{CUSTOMER_MGMT_URL}/health", timeout=2.0
            )
            if response.status_code != 200:
                raise ExternalServiceError(
                    "CustomerMgmt", f"Health check failed: {response.status_code}"
                )
        except Exception as e:
            logger.warning(f"Customer Mgmt API readiness check failed: {e}")
            metrics.set_dependency_health("customer-mgmt", False)
            raise ExternalServiceError("CustomerMgmt", str(e))

        metrics.set_dependency_health("customer-mgmt", True)
        return HealthResponse(
            status="ready",
            service="customer-facing",
            timestamp=datetime.utcnow().isoformat(),
        )
    except TaskAuraException as e:
        logger.error(f"Readiness check failed: {e.message}")
        raise HTTPException(status_code=e.status_code, detail=e.message)


# ============================================================================
# API Endpoints
# ============================================================================


@app.post("/buy", tags=["Purchases"], status_code=202)
async def buy_item(
    purchase: PurchaseRequest,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Accept a purchase request and publish to Kafka for async processing.

    Args:
        purchase: PurchaseRequest containing username, userid, price
        X-API-Key: Valid API key in header

    Returns:
        202 Accepted - Message queued for processing

    Raises:
        400: Validation error (invalid data)
        401: Authentication error (missing/invalid API key)
        503: Service unavailable (Kafka error)
    """
    try:
        # Validate API key
        await validate_api_key(api_key)

        # Create message for Kafka
        message = PurchaseMessage(
            username=purchase.username,
            userid=purchase.userid,
            price=purchase.price,
            timestamp=datetime.utcnow().isoformat(),
        )

        logger.info(
            f"Purchase request received",
            extra={"extra_data": {"userid": purchase.userid, "price": purchase.price}},
        )

        # Publish to Kafka (async)
        try:
            await kafka_producer.send_and_wait(
                KAFKA_TOPIC,
                value=message.model_dump(),
                key=purchase.userid.encode("utf-8"),
            )
            logger.info(f"Message published to Kafka topic '{KAFKA_TOPIC}'")
            metrics.kafka_messages_consumed.labels(
                topic=KAFKA_TOPIC, status="sent"
            ).inc()
            metrics.purchases_total.labels(status="accepted").inc()
            metrics.purchase_amount.labels(currency="USD").observe(purchase.price)
        except Exception as e:
            logger.error(f"Failed to publish to Kafka: {e}")
            metrics.kafka_messages_consumed.labels(
                topic=KAFKA_TOPIC, status="failed"
            ).inc()
            metrics.purchases_total.labels(status="failed").inc()
            raise KafkaError(str(e))

        return {
            "status": "accepted",
            "userid": purchase.userid,
            "message": "Purchase request queued for processing",
        }

    except ValidationError as e:
        logger.warning(f"Validation error: {e.message}")
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except AuthenticationError as e:
        logger.warning(f"Authentication error: {e.message}")
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except KafkaError as e:
        logger.error(f"Kafka error: {e.message}")
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except TaskAuraException as e:
        logger.error(f"Error: {e.message}")
        raise HTTPException(status_code=e.status_code, detail=e.message)


@app.get("/purchases/{userid}", tags=["Purchases"])
async def get_user_purchases(
    userid: str, api_key: str = Header(None, alias="X-API-Key")
):
    """
    Retrieve all purchases for a user from Customer Management API.

    Args:
        userid: User ID to retrieve purchases for
        X-API-Key: Valid API key in header

    Returns:
        List of purchases for the user

    Raises:
        400: Validation error
        401: Authentication error
        503: Service unavailable (Customer Mgmt API error)
    """
    try:
        # Validate API key
        await validate_api_key(api_key)

        # Validate userid
        if not userid or len(userid) > 255:
            raise ValidationError("Invalid userid")

        logger.info(f"Get purchases request for userid: {userid}")

        # Call Customer Management API
        try:
            response = await httpx_client.get(
                f"{CUSTOMER_MGMT_URL}/users/{userid}/purchases", timeout=REQUEST_TIMEOUT
            )

            if response.status_code == 404:
                logger.info(f"User not found: {userid}")
                return {"userid": userid, "purchases": []}

            if response.status_code != 200:
                logger.error(f"Customer Mgmt API error: {response.status_code}")
                metrics.set_dependency_health("customer-mgmt", False)
                raise ExternalServiceError(
                    "CustomerManagementAPI", f"API returned {response.status_code}"
                )

            metrics.set_dependency_health("customer-mgmt", True)
            purchases = response.json()

            logger.info(
                f"Retrieved {len(purchases.get('purchases', []))} purchases for userid: {userid}"
            )
            return purchases

        except Exception as e:
            logger.error(f"Failed to retrieve purchases from Customer Mgmt API: {e}")
            raise ExternalServiceError("CustomerManagementAPI", str(e))

    except ValidationError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except AuthenticationError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except ExternalServiceError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)


# ============================================================================
# Metrics Endpoint
# ============================================================================


@app.get("/metrics", tags=["Monitoring"], include_in_schema=False)
async def metrics_endpoint():
    """
    Prometheus metrics endpoint.
    Scraped by Prometheus to collect metrics.
    """
    from fastapi.responses import PlainTextResponse

    return PlainTextResponse(
        generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST
    )


# ============================================================================
# Error Handlers
# ============================================================================


@app.exception_handler(TaskAuraException)
async def task_aura_exception_handler(request: Request, exc: TaskAuraException):
    """Handle Task Aura exceptions."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.error_code,
            "message": exc.message,
            "timestamp": datetime.utcnow().isoformat(),
        },
    )


if __name__ == "__main__":
    import uvicorn

    # Run with SSL/mTLS if certificates are provided
    ssl_keyfile = os.getenv("SSL_KEY_PATH")
    ssl_certfile = os.getenv("SSL_CERT_PATH")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=API_PORT,
        ssl_keyfile=ssl_keyfile,
        ssl_certfile=ssl_certfile,
        log_config=None,  # Use our structured logging
    )
