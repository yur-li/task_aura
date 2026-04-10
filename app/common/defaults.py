"""Shared default configuration values for Task Aura services."""

SERVICE_DEFAULTS = {
    "LOG_LEVEL": "INFO",
    "KAFKA_BROKERS": "kafka:9092",
    "KAFKA_TOPIC": "purchases",
    "KAFKA_SASL_MECHANISM": "PLAIN",
    "KAFKA_SASL_PLAIN_PASSWORD": "admin",
    "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
    "MONGODB_DATABASE": "task_aura",
    "MONGODB_COLLECTION": "purchases",
}

CUSTOMER_FACING_DEFAULTS = {
    "API_PORT": "8080",
    "METRICS_PORT": "8001",
    "KAFKA_SASL_PLAIN_USERNAME": "user1",
    "CUSTOMER_MGMT_URL": "https://customer-mgmt:8081",
    "REQUEST_TIMEOUT": "10.0",
    "VALID_API_KEYS": "dev-key-123",
    "CORS_ALLOWED_ORIGINS": (
        "http://localhost:3001,http://127.0.0.1:3001,"
        "http://localhost:8080,http://127.0.0.1:8080"
    ),
}

CUSTOMER_MGMT_DEFAULTS = {
    "API_PORT": "8081",
    "METRICS_PORT": "8002",
    "MONGODB_CONNECTION_STRING": (
        "mongodb://admin:admin@mongodb:27017/task_aura?authSource=admin"
    ),
    "KAFKA_SASL_PLAIN_USERNAME": "admin",
    "REQUEST_LIMIT": "1000",
    "KAFKA_BATCH_SIZE": 100,
    "KAFKA_BATCH_TIMEOUT_SECONDS": 5,
}
