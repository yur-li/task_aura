"""
MongoDB Database Operations for Task Aura Customer Management Service
"""

from typing import List, Dict, Any
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.common.logging_setup import get_logger
from app.common.errors import DatabaseError

logger = get_logger("customer-mgmt")


class PurchaseDatabase:
    """Database operations for purchases."""

    def __init__(self, db: AsyncIOMotorDatabase, collection_name: str = "purchases"):
        """
        Initialize database operations.

        Args:
            db: Motor MongoDB database instance
            collection_name: Name of the collection
        """
        self.db = db
        self.collection_name = collection_name
        self.collection = db[collection_name]

    async def initialize(self):
        """Initialize database indexes."""
        try:
            logger.info(f"Initializing indexes for collection: {self.collection_name}")

            # Index on userid for fast lookups
            await self.collection.create_index("userid", name="idx_userid")

            # Index on timestamp for range queries
            await self.collection.create_index("timestamp", name="idx_timestamp")

            # Compound index on userid + timestamp for efficient sorting
            await self.collection.create_index(
                [("userid", 1), ("timestamp", -1)], name="idx_userid_timestamp"
            )

            logger.info("Database indexes created successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database indexes: {e}")
            raise DatabaseError(str(e))

    async def get_user_purchases(
        self, userid: str, limit: int = 1000, skip: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Retrieve all purchases for a user.

        Args:
            userid: User ID to retrieve purchases for
            limit: Maximum number of records to return
            skip: Number of records to skip (for pagination)

        Returns:
            List of purchase documents

        Raises:
            DatabaseError: If query fails
        """
        try:
            logger.debug(f"Querying purchases for userid: {userid}")

            query = {"userid": userid}
            cursor = (
                self.collection.find(query)
                .sort("timestamp", -1)
                .skip(skip)
                .limit(limit)
            )

            purchases = await cursor.to_list(length=limit)

            logger.info(f"Retrieved {len(purchases)} purchases for userid: {userid}")
            return purchases

        except Exception as e:
            logger.error(f"Failed to retrieve purchases from MongoDB: {e}")
            raise DatabaseError(str(e))

    async def get_user_purchase_count(self, userid: str) -> int:
        """
        Get total purchase count for a user.

        Args:
            userid: User ID

        Returns:
            Total number of purchases

        Raises:
            DatabaseError: If query fails
        """
        try:
            count = await self.collection.count_documents({"userid": userid})
            logger.debug(f"Purchase count for userid {userid}: {count}")
            return count

        except Exception as e:
            logger.error(f"Failed to count purchases: {e}")
            raise DatabaseError(str(e))

    async def get_purchase_stats(self, userid: str) -> Dict[str, Any]:
        """
        Get aggregated statistics for a user's purchases.

        Args:
            userid: User ID

        Returns:
            Dictionary with stats: total_purchases, total_amount, avg_price, latest_purchase

        Raises:
            DatabaseError: If aggregation fails
        """
        try:
            pipeline = [
                {"$match": {"userid": userid}},
                {
                    "$group": {
                        "_id": "$userid",
                        "total_purchases": {"$sum": 1},
                        "total_amount": {"$sum": "$price"},
                        "avg_price": {"$avg": "$price"},
                        "min_price": {"$min": "$price"},
                        "max_price": {"$max": "$price"},
                        "latest_purchase": {"$max": "$timestamp"},
                    }
                },
            ]

            cursor = self.collection.aggregate(pipeline)
            results = await cursor.to_list(length=1)

            if not results:
                logger.debug(f"No purchases found for userid: {userid}")
                return {
                    "userid": userid,
                    "total_purchases": 0,
                    "total_amount": 0.0,
                    "avg_price": 0.0,
                }

            stats = results[0]
            logger.info(f"Purchase stats for {userid}: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Failed to get purchase stats: {e}")
            raise DatabaseError(str(e))

    async def get_all_user_purchases_with_count(
        self, userid: str, limit: int = 1000, skip: int = 0
    ) -> Dict[str, Any]:
        """
        Get purchases with total count (for pagination).

        Args:
            userid: User ID
            limit: Maximum records to return
            skip: Records to skip

        Returns:
            Dictionary with purchases, total_count, limit, skip

        Raises:
            DatabaseError: If queries fail
        """
        try:
            purchases = await self.get_user_purchases(userid, limit, skip)
            total_count = await self.get_user_purchase_count(userid)

            return {
                "userid": userid,
                "purchases": purchases,
                "total_count": total_count,
                "limit": limit,
                "skip": skip,
                "has_more": (skip + limit) < total_count,
            }

        except Exception as e:
            logger.error(f"Failed to get user purchases with count: {e}")
            raise

    async def health_check(self) -> bool:
        """
        Check if database is accessible.

        Returns:
            True if healthy, raises exception otherwise

        Raises:
            DatabaseError: If health check fails
        """
        try:
            # Try to run a simple command
            result = await self.db.command("ping")
            if result.get("ok") == 1:
                logger.debug("Database health check passed")
                return True
            raise DatabaseError("Ping command failed")
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            raise DatabaseError(str(e))


def create_database(connection_string: str, database_name: str) -> AsyncIOMotorDatabase:
    """
    Create and return a Motor MongoDB database instance.

    Args:
        connection_string: MongoDB connection string (e.g., "mongodb://user:pass@host:27017")
        database_name: Name of the database

    Returns:
        Motor AsyncIOMotorDatabase instance
    """
    try:
        logger.info(f"Connecting to MongoDB: {database_name}")
        client = AsyncIOMotorClient(connection_string)
        db = client[database_name]
        logger.info(f"Connected to MongoDB database: {database_name}")
        return db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise DatabaseError(str(e))
