"""
Customer Management API Service for Task Aura
Handles customer purchase queries and Kafka message consumption.
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import os
import time
from datetime import datetime
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel

from app.common.logging_setup import setup_logging
from app.common.metrics import init_metrics, get_metrics, start_metrics_server
from app.common.defaults import SERVICE_DEFAULTS, CUSTOMER_MGMT_DEFAULTS

# Initialize logging and metrics FIRST before importing modules that use them
logger = setup_logging(
    "customer-mgmt", level=os.getenv("LOG_LEVEL", SERVICE_DEFAULTS["LOG_LEVEL"])
)
init_metrics("customer-mgmt")

# Now safe to import other modules
from app.common.errors import (
    TaskAuraException,
    ExternalServiceError,
)
from app.customer_mgmt.db import create_database, PurchaseDatabase
from app.customer_mgmt.kafka_consumer import PurchaseConsumer, ConsumerManager

# ============================================================================
# Configuration & Setup
# ============================================================================

# Get metrics instance (already initialized above)
metrics = get_metrics()

# Load configuration from environment
API_PORT = int(os.getenv("API_PORT", CUSTOMER_MGMT_DEFAULTS["API_PORT"]))
METRICS_PORT = int(os.getenv("METRICS_PORT", CUSTOMER_MGMT_DEFAULTS["METRICS_PORT"]))

# MongoDB configuration
MONGODB_CONNECTION_STRING = os.getenv(
    "MONGODB_CONNECTION_STRING",
    CUSTOMER_MGMT_DEFAULTS["MONGODB_CONNECTION_STRING"],
)
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", SERVICE_DEFAULTS["MONGODB_DATABASE"])
MONGODB_COLLECTION = os.getenv(
    "MONGODB_COLLECTION", SERVICE_DEFAULTS["MONGODB_COLLECTION"]
)

# Kafka configuration
KAFKA_BROKERS = os.getenv(
    "KAFKA_BROKERS", SERVICE_DEFAULTS["KAFKA_BROKERS"]
).split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", SERVICE_DEFAULTS["KAFKA_TOPIC"])
KAFKA_SASL_MECHANISM = os.getenv(
    "KAFKA_SASL_MECHANISM", SERVICE_DEFAULTS["KAFKA_SASL_MECHANISM"]
)
KAFKA_SASL_PLAIN_USERNAME = os.getenv(
    "KAFKA_SASL_PLAIN_USERNAME", CUSTOMER_MGMT_DEFAULTS["KAFKA_SASL_PLAIN_USERNAME"]
)
KAFKA_SASL_PLAIN_PASSWORD = os.getenv(
    "KAFKA_SASL_PLAIN_PASSWORD", SERVICE_DEFAULTS["KAFKA_SASL_PLAIN_PASSWORD"]
)
KAFKA_SECURITY_PROTOCOL = os.getenv(
    "KAFKA_SECURITY_PROTOCOL", SERVICE_DEFAULTS["KAFKA_SECURITY_PROTOCOL"]
)

# Service-level configuration
REQUEST_LIMIT = int(
    os.getenv("REQUEST_LIMIT", CUSTOMER_MGMT_DEFAULTS["REQUEST_LIMIT"])
)

logger.info(f"Starting Customer Management Service on port {API_PORT}")
logger.info(f"MongoDB: {MONGODB_DATABASE}")
logger.info(f"Kafka brokers: {KAFKA_BROKERS}")

# ============================================================================
# Data Models
# ============================================================================


class PurchasesListResponse(BaseModel):
    """List of purchases for a user."""

    userid: str
    purchases: list
    total_count: int
    limit: int
    skip: int
    has_more: bool


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    service: str
    timestamp: str


# ============================================================================
# FastAPI Application
# ============================================================================

app = FastAPI(
    title="Task Aura - Customer Management Service",
    description="Handles customer queries and Kafka message consumption",
    version="1.0.0",
)


def _tracked_endpoint(request) -> Optional[str]:
    route = request.scope.get("route")
    endpoint = getattr(route, "path", request.url.path)

    if endpoint in {"/health", "/ready", "/metrics"}:
        return None

    return endpoint


@app.middleware("http")
async def collect_http_metrics(request, call_next):
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

# Global state
db: Optional[AsyncIOMotorDatabase] = None
purchase_db: Optional[PurchaseDatabase] = None
consumer_manager: Optional[ConsumerManager] = None


# ============================================================================
# Startup & Shutdown Events
# ============================================================================


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    global db, purchase_db, consumer_manager

    logger.info("Startup event: Initializing services")

    # Start Prometheus metrics server
    try:
        start_metrics_server(METRICS_PORT, registry=metrics.registry)
        logger.info(f"Prometheus metrics server started on port {METRICS_PORT}")
        metrics.set_service_health(True)
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")

    # Connect to MongoDB
    try:
        db = create_database(MONGODB_CONNECTION_STRING, MONGODB_DATABASE)
        purchase_db = PurchaseDatabase(db, MONGODB_COLLECTION)
        await purchase_db.initialize()
        logger.info("MongoDB initialized successfully")
        metrics.set_dependency_health("mongodb", True)
    except Exception as e:
        logger.error(f"Failed to initialize MongoDB: {e}")
        metrics.set_dependency_health("mongodb", False)
        raise

    # Initialize Kafka consumer
    try:
        kafka_config = {
            "sasl_mechanism": KAFKA_SASL_MECHANISM,
            "sasl_plain_username": KAFKA_SASL_PLAIN_USERNAME,
            "sasl_plain_password": KAFKA_SASL_PLAIN_PASSWORD,
            "security_protocol": KAFKA_SECURITY_PROTOCOL,
        }

        consumer = PurchaseConsumer(
            bootstrap_servers=KAFKA_BROKERS,
            topic=KAFKA_TOPIC,
            db=db,
            collection_name=MONGODB_COLLECTION,
            batch_size=CUSTOMER_MGMT_DEFAULTS["KAFKA_BATCH_SIZE"],
            batch_timeout_seconds=CUSTOMER_MGMT_DEFAULTS[
                "KAFKA_BATCH_TIMEOUT_SECONDS"
            ],
            **kafka_config,
        )

        consumer_manager = ConsumerManager(consumer)
        await consumer_manager.start()
        logger.info("Kafka consumer started successfully")
        metrics.set_dependency_health("kafka", True)
    except Exception as e:
        logger.error(f"Failed to initialize Kafka consumer: {e}")
        metrics.set_dependency_health("kafka", False)
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown."""
    global consumer_manager

    logger.info("Shutdown event: Cleaning up resources")

    # Stop Kafka consumer
    if consumer_manager:
        await consumer_manager.stop()
        logger.info("Kafka consumer stopped")


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
        status="ok", service="customer-mgmt", timestamp=datetime.utcnow().isoformat()
    )


@app.get("/ready", tags=["Health"], response_model=HealthResponse)
async def readiness_check() -> HealthResponse:
    """
    Readiness probe: Service is ready to accept traffic.
    Checks: MongoDB connectivity, Kafka connectivity
    """
    try:
        if db is None or purchase_db is None or consumer_manager is None:
            raise ExternalServiceError(
                "Initialization", "Services not fully initialized"
            )

        # Check MongoDB health
        try:
            await purchase_db.health_check()
            metrics.set_dependency_health("mongodb", True)
        except Exception as e:
            logger.warning(f"MongoDB health check failed: {e}")
            metrics.set_dependency_health("mongodb", False)
            raise ExternalServiceError("MongoDB", str(e))

        # Check Kafka consumer is running
        if not consumer_manager.consumer.running:
            raise ExternalServiceError("Kafka", "Consumer not running")

        metrics.set_dependency_health("kafka", True)

        return HealthResponse(
            status="ready",
            service="customer-mgmt",
            timestamp=datetime.utcnow().isoformat(),
        )

    except TaskAuraException as e:
        logger.error(f"Readiness check failed: {e.message}")
        raise HTTPException(status_code=e.status_code, detail=e.message)


# ============================================================================
# API Endpoints
# ============================================================================


@app.get("/users/{userid}/purchases", tags=["Purchases"])
async def get_user_purchases(userid: str, limit: int = 100, skip: int = 0):
    """
    Retrieve all purchases for a user.

    Args:
        userid: User ID
        limit: Maximum records to return (default: 100, max: 1000)
        skip: Records to skip for pagination (default: 0)

    Returns:
        List of purchases with pagination info

    Raises:
        400: Validation error (bad parameters)
        503: Service unavailable (MongoDB error)
    """
    try:
        # Validate parameters
        if not userid or len(userid) > 255:
            raise ValueError("Invalid userid")

        limit = min(int(limit), REQUEST_LIMIT)
        if limit <= 0:
            limit = 100

        skip = max(int(skip), 0)

        logger.info(
            f"Getting purchases for userid: {userid}, limit: {limit}, skip: {skip}"
        )

        # Query database
        try:
            result = await purchase_db.get_all_user_purchases_with_count(
                userid=userid, limit=limit, skip=skip
            )

            # Clean up MongoDB document IDs for JSON response
            for purchase in result["purchases"]:
                if "_id" in purchase:
                    purchase["_id"] = str(purchase["_id"])

            logger.info(f"Retrieved purchases for {userid}")
            return result

        except Exception as e:
            logger.error(f"Database query failed: {e}")
            metrics.set_dependency_health("mongodb", False)
            raise ExternalServiceError("MongoDB", str(e))

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ExternalServiceError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)


@app.get("/users/{userid}/stats", tags=["Analytics"])
async def get_user_stats(userid: str):
    """
    Get purchase statistics for a user.

    Args:
        userid: User ID

    Returns:
        Statistics: total_purchases, total_amount, avg_price, min/max price

    Raises:
        400: Validation error
        503: Service unavailable (MongoDB error)
    """
    try:
        if not userid or len(userid) > 255:
            raise ValueError("Invalid userid")

        logger.info(f"Getting stats for userid: {userid}")

        try:
            stats = await purchase_db.get_purchase_stats(userid)
            logger.info(f"Retrieved stats for {userid}")
            return stats

        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            metrics.set_dependency_health("mongodb", False)
            raise ExternalServiceError("MongoDB", str(e))

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ExternalServiceError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)


# ============================================================================
# Metrics Endpoint
# ============================================================================


@app.get("/metrics", tags=["Monitoring"], include_in_schema=False)
async def metrics_endpoint():
    """Prometheus metrics endpoint."""
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    from fastapi.responses import PlainTextResponse

    return PlainTextResponse(
        generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST
    )


# ============================================================================
# Error Handlers
# ============================================================================


@app.exception_handler(TaskAuraException)
async def task_aura_exception_handler(request, exc: TaskAuraException):
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

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=API_PORT,
        log_config=None,  # Use our structured logging
    )
