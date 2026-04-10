"""
Kafka Consumer Logic for Task Aura Customer Management Service
Consumes purchase messages and writes to MongoDB with batching.
"""

import asyncio
import json
from typing import List, Dict, Any
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.common.logging_setup import get_logger
from app.common.metrics import get_metrics
from app.common.errors import KafkaError, DatabaseError

logger = get_logger("customer-mgmt")

# Lazy-load metrics to avoid initialization errors
def _get_metrics():
    """Lazy-load metrics instance."""
    return get_metrics()


class PurchaseConsumer:
    """
    Consumes purchase messages from Kafka and writes to MongoDB.
    Implements batch writing for efficiency and reliability.
    """

    def __init__(
        self,
        bootstrap_servers: List[str],
        topic: str,
        db: AsyncIOMotorDatabase,
        collection_name: str = "purchases",
        batch_size: int = 100,
        batch_timeout_seconds: int = 5,
        **kafka_config,
    ):
        """
        Initialize Kafka consumer.

        Args:
            bootstrap_servers: List of Kafka broker addresses
            topic: Topic to consume from
            db: Motor MongoDB database instance
            collection_name: MongoDB collection name
            batch_size: Messages to batch before writing to DB
            batch_timeout_seconds: Max time to wait before writing batch
            **kafka_config: Additional Kafka config (sasl_mechanism, security_protocol, etc.)
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.db = db
        self.collection_name = collection_name
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds
        self.kafka_config = kafka_config

        self.consumer: AIOKafkaConsumer = None
        self.collection = None
        self.running = False
        self.message_batch: List[Dict[str, Any]] = []
        self.last_batch_time = datetime.utcnow()

    async def start(self):
        """Start the Kafka consumer."""
        try:
            logger.info(f"Starting Kafka consumer for topic: {self.topic}")

            # Initialize MongoDB collection
            self.collection = self.db[self.collection_name]

            # Create unique index on (userid, timestamp) for query optimization
            # Note: In production, consider compound index strategies
            await self.collection.create_index("userid", name="idx_userid")
            logger.info(f"MongoDB collection '{self.collection_name}' ready")

            # Initialize Kafka consumer
            # Consumer group: "purchase-consumer"
            # Heartbeat interval: 3s (default is 3s anyway)
            # Session timeout: 10s
            self.consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id="purchase-consumer",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                heartbeat_interval_ms=3000,  # 3 seconds
                session_timeout_ms=10000,  # 10 seconds
                max_poll_interval_ms=300000,  # 5 minutes
                **self.kafka_config,
            )

            await self.consumer.start()
            logger.info("Kafka consumer started successfully")
            _get_metrics().set_dependency_health("kafka", True)

            self.running = True
        except Exception as e:
            logger.error(f"Failed to start Kafka consumer: {e}")
            _get_metrics().set_dependency_health("kafka", False)
            raise KafkaError(str(e))

    async def stop(self):
        """Stop the Kafka consumer gracefully."""
        logger.info("Stopping Kafka consumer")
        self.running = False

        # Flush any pending messages
        if self.message_batch:
            await self._write_batch_to_db()

        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped")

    async def consume_messages(self):
        """
        Main consumer loop - continuously consumes and batches messages.
        Call this as a long-running task.
        """
        try:
            async for message in self.consumer:
                try:
                    purchase_data = message.value
                    partition = TopicPartition(message.topic, message.partition)

                    # Validate message structure
                    required_fields = {"username", "userid", "price", "timestamp"}
                    if not all(field in purchase_data for field in required_fields):
                        logger.error(f"Invalid message structure: {purchase_data}")
                        _get_metrics().kafka_messages_consumed.labels(
                            topic=self.topic, status="failed"
                        ).inc()
                        continue

                    # Add MongoDB metadata
                    purchase_data["_received_at"] = datetime.utcnow().isoformat()

                    # Add to batch
                    self.message_batch.append(purchase_data)

                    highwater = self.consumer.highwater(partition)
                    if highwater is not None:
                        lag = max(0, highwater - message.offset - 1)
                        _get_metrics().kafka_consumer_lag.labels(
                            topic=message.topic,
                            partition=str(message.partition),
                        ).set(lag)

                    logger.debug(
                        f"Message added to batch ({len(self.message_batch)}/{self.batch_size})"
                    )

                    # Check if we should write batch
                    should_write = (
                        len(self.message_batch) >= self.batch_size
                        or (datetime.utcnow() - self.last_batch_time).total_seconds()
                        >= self.batch_timeout_seconds
                    )

                    if should_write:
                        await self._write_batch_to_db()

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    _get_metrics().kafka_messages_consumed.labels(
                        topic=self.topic, status="failed"
                    ).inc()
                    continue

        except asyncio.CancelledError:
            logger.info("Consumer task cancelled")
            await self.stop()
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}")
            _get_metrics().set_dependency_health("kafka", False)
            raise KafkaError(str(e))

    async def _write_batch_to_db(self):
        """Write accumulated batch to MongoDB."""
        if not self.message_batch:
            return

        batch_size = len(self.message_batch)

        try:
            logger.info(f"Writing batch of {batch_size} messages to MongoDB")

            # Insert all messages in a single bulk write
            result = await self.collection.insert_many(
                self.message_batch, ordered=False
            )

            logger.info(
                f"Batch written successfully: {len(result.inserted_ids)} messages"
            )
            _get_metrics().kafka_messages_consumed.labels(
                topic=self.topic, status="success"
            ).add(batch_size)
            _get_metrics().kafka_batch_size.labels(topic=self.topic).observe(batch_size)

            # Reset batch
            self.message_batch = []
            self.last_batch_time = datetime.utcnow()

        except Exception as e:
            logger.error(f"Failed to write batch to MongoDB: {e}")
            _get_metrics().kafka_messages_consumed.labels(
                topic=self.topic, status="failed"
            ).add(batch_size)
            raise DatabaseError(str(e))


class ConsumerManager:
    """Manages the lifecycle of the Kafka consumer."""

    def __init__(self, consumer: PurchaseConsumer):
        self.consumer = consumer
        self.consumer_task: asyncio.Task = None

    async def start(self):
        """Start the consumer."""
        await self.consumer.start()
        self.consumer_task = asyncio.create_task(self.consumer.consume_messages())
        logger.info("Consumer manager started")

    async def stop(self):
        """Stop the consumer gracefully."""
        await self.consumer.stop()
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass
        logger.info("Consumer manager stopped")
