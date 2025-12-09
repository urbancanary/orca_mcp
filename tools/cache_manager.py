"""
Redis Cache Manager for Orca MCP

Provides intelligent caching layer for BigQuery queries to reduce costs and improve performance.

Cache Strategy:
- Universe data (agg_analysis_data): 1 hour TTL
- Client holdings: 5 min TTL
- Transactions: 15 min TTL
- RVM analytics: 30 min TTL

Key Patterns:
- universe:all:full → agg_analysis_data
- holdings:{client_id}:all → current_holdings
- transactions:{client_id}:{year} → filtered transactions
- rvm:{client_id}:latest → RVM analytics
- query:{query_hash} → arbitrary queries
"""

import os
import json
import hashlib
import logging
from typing import Any, Dict, Optional, List
from datetime import timedelta

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logging.warning("Redis not installed - caching disabled")

logger = logging.getLogger(__name__)


class CacheManager:
    """Redis-based cache manager with intelligent TTL and invalidation"""

    # Default TTLs (in seconds)
    TTL_UNIVERSE = 3600  # 1 hour
    TTL_HOLDINGS = 300   # 5 minutes
    TTL_TRANSACTIONS = 900  # 15 minutes
    TTL_RVM = 1800       # 30 minutes
    TTL_DEFAULT = 300    # 5 minutes

    def __init__(self, redis_url: Optional[str] = None, enabled: bool = True):
        """
        Initialize cache manager

        Args:
            redis_url: Redis connection URL (default: from REDIS_URL env var)
            enabled: Whether caching is enabled (default: True)
        """
        self.enabled = enabled and REDIS_AVAILABLE
        self.redis_client = None

        if not REDIS_AVAILABLE:
            logger.warning("Redis not available - caching disabled")
            self.enabled = False
            return

        if not self.enabled:
            logger.info("Caching explicitly disabled")
            return

        # Get Redis URL from env or parameter
        redis_url = redis_url or os.getenv('REDIS_URL')

        if not redis_url:
            logger.warning("No REDIS_URL configured - caching disabled")
            self.enabled = False
            return

        try:
            # Connect to Redis
            self.redis_client = redis.from_url(
                redis_url,
                decode_responses=True,  # Automatically decode bytes to strings
                socket_connect_timeout=5,
                socket_timeout=5
            )

            # Test connection
            self.redis_client.ping()
            logger.info("✓ Redis connection established")

        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            logger.warning("Falling back to no-cache mode")
            self.enabled = False
            self.redis_client = None

    def get(self, key: str) -> Optional[Any]:
        """
        Get cached data

        Args:
            key: Cache key

        Returns:
            Cached data (deserialized from JSON) or None if miss/disabled
        """
        if not self.enabled or not self.redis_client:
            return None

        try:
            data = self.redis_client.get(key)
            if data:
                logger.debug(f"Cache HIT: {key}")
                return json.loads(data)
            else:
                logger.debug(f"Cache MISS: {key}")
                return None

        except Exception as e:
            logger.error(f"Cache read error for {key}: {e}")
            return None

    def set(self, key: str, data: Any, ttl: int = TTL_DEFAULT):
        """
        Cache data with TTL

        Args:
            key: Cache key
            data: Data to cache (will be JSON serialized)
            ttl: Time-to-live in seconds (default: 5 min)
        """
        if not self.enabled or not self.redis_client:
            return

        try:
            serialized = json.dumps(data, default=str)  # default=str for datetimes
            self.redis_client.setex(key, ttl, serialized)
            logger.debug(f"Cached: {key} (TTL: {ttl}s)")

        except Exception as e:
            logger.error(f"Cache write error for {key}: {e}")

    def invalidate(self, pattern: str) -> int:
        """
        Invalidate all keys matching pattern

        Args:
            pattern: Redis pattern (e.g., "universe:*", "holdings:client001:*")

        Returns:
            Number of keys deleted
        """
        if not self.enabled or not self.redis_client:
            return 0

        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                count = self.redis_client.delete(*keys)
                logger.info(f"Invalidated {count} keys matching '{pattern}'")
                return count
            else:
                logger.debug(f"No keys found matching '{pattern}'")
                return 0

        except Exception as e:
            logger.error(f"Cache invalidation error for {pattern}: {e}")
            return 0

    def invalidate_client(self, client_id: str):
        """
        Invalidate all cached data for a specific client

        Args:
            client_id: Client ID to invalidate
        """
        patterns = [
            f"holdings:{client_id}:*",
            f"transactions:{client_id}:*",
            f"rvm:{client_id}:*",
            f"query:*{client_id}*"  # Queries mentioning this client
        ]

        total = 0
        for pattern in patterns:
            total += self.invalidate(pattern)

        logger.info(f"Invalidated {total} keys for client {client_id}")
        return total

    def flush_all(self) -> bool:
        """
        Flush entire cache (use with caution!)

        Returns:
            True if successful
        """
        if not self.enabled or not self.redis_client:
            return False

        try:
            self.redis_client.flushdb()
            logger.warning("⚠️  Flushed entire cache")
            return True

        except Exception as e:
            logger.error(f"Cache flush error: {e}")
            return False

    def query_hash(self, sql: str, params: Optional[Dict] = None) -> str:
        """
        Generate deterministic hash for query caching

        Args:
            sql: SQL query string
            params: Query parameters (optional)

        Returns:
            16-character hash string
        """
        # Normalize SQL (remove extra whitespace)
        normalized_sql = ' '.join(sql.split())

        # Include params in hash
        if params:
            query_str = f"{normalized_sql}:{sorted(params.items())}"
        else:
            query_str = normalized_sql

        # Generate hash
        hash_obj = hashlib.sha256(query_str.encode())
        return hash_obj.hexdigest()[:16]

    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics

        Returns:
            Dictionary with cache stats (hits, misses, memory, etc.)
        """
        if not self.enabled or not self.redis_client:
            return {"enabled": False}

        try:
            info = self.redis_client.info('stats')
            keyspace = self.redis_client.info('keyspace')

            # Calculate hit rate
            hits = info.get('keyspace_hits', 0)
            misses = info.get('keyspace_misses', 0)
            total_requests = hits + misses
            hit_rate = (hits / total_requests * 100) if total_requests > 0 else 0

            # Count keys
            db_info = keyspace.get('db0', {})
            total_keys = db_info.get('keys', 0) if isinstance(db_info, dict) else 0

            return {
                "enabled": True,
                "total_keys": total_keys,
                "keyspace_hits": hits,
                "keyspace_misses": misses,
                "hit_rate_percent": round(hit_rate, 2),
                "memory_used": self.redis_client.info('memory').get('used_memory_human', 'N/A')
            }

        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {"enabled": True, "error": str(e)}

    def health_check(self) -> bool:
        """
        Check if Redis is healthy

        Returns:
            True if Redis is accessible
        """
        if not self.enabled or not self.redis_client:
            return False

        try:
            return self.redis_client.ping()
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False


# Global cache manager instance (lazy-initialized)
_cache_manager: Optional[CacheManager] = None


def get_cache_manager() -> CacheManager:
    """
    Get global cache manager instance (singleton)

    Returns:
        CacheManager instance
    """
    global _cache_manager

    if _cache_manager is None:
        _cache_manager = CacheManager()

    return _cache_manager


# Convenience functions
def get_cache(key: str) -> Optional[Any]:
    """Get cached data"""
    return get_cache_manager().get(key)


def set_cache(key: str, data: Any, ttl: int = CacheManager.TTL_DEFAULT):
    """Cache data with TTL"""
    get_cache_manager().set(key, data, ttl)


def invalidate_cache(pattern: str) -> int:
    """Invalidate keys matching pattern"""
    return get_cache_manager().invalidate(pattern)


def get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics"""
    return get_cache_manager().get_stats()


if __name__ == "__main__":
    # Test cache manager
    import sys

    logging.basicConfig(level=logging.DEBUG)

    # Initialize
    cache = CacheManager()
    print(f"Cache enabled: {cache.enabled}")

    if cache.enabled:
        # Test set/get
        cache.set("test:key1", {"foo": "bar"}, ttl=60)
        result = cache.get("test:key1")
        print(f"Test result: {result}")

        # Test stats
        stats = cache.get_stats()
        print(f"Cache stats: {json.dumps(stats, indent=2)}")

        # Test invalidation
        cache.invalidate("test:*")
        print("Invalidated test:* keys")
    else:
        print("Cache not available - set REDIS_URL environment variable")
