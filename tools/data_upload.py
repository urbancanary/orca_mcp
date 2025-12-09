"""
Data Upload Tools for Orca MCP

Handles BigQuery write operations with automatic cache invalidation.
"""

import os
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

try:
    from google.cloud import bigquery
    from google.cloud.exceptions import GoogleCloudError
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    bigquery = None

try:
    from ..client_config import get_client_config
    from .data_access import setup_bigquery_credentials
    from .cache_manager import get_cache_manager
except ImportError:
    # When deployed standalone (not as package)
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from client_config import get_client_config
    from .data_access import setup_bigquery_credentials
    from .cache_manager import get_cache_manager

logger = logging.getLogger(__name__)


def upload_table(
    table_name: str,
    source_file: str,
    client_id: str = None,
    format: str = "parquet",
    write_disposition: str = "WRITE_TRUNCATE",
    invalidate_cache: bool = True
) -> Dict[str, Any]:
    """
    Upload data file to BigQuery table with automatic cache invalidation

    Args:
        table_name: Target table name (e.g., 'agg_analysis_data')
        source_file: Path to source file (Parquet or CSV)
        client_id: Client identifier (uses default if None)
        format: File format ('parquet' or 'csv')
        write_disposition: 'WRITE_TRUNCATE' (replace) or 'WRITE_APPEND' (append)
        invalidate_cache: Whether to invalidate cache after upload (default: True)

    Returns:
        {
            "table": "agg_analysis_data",
            "rows_written": 32514,
            "bytes_processed": 15728640,
            "duration_seconds": 12.4,
            "cache_invalidated": True,
            "cache_keys_deleted": 15
        }

    Raises:
        FileNotFoundError: Source file doesn't exist
        ValueError: Invalid format or write disposition
        RuntimeError: BigQuery upload failed
    """
    if not BIGQUERY_AVAILABLE:
        raise ImportError(
            "BigQuery is not available. Install with: pip install google-cloud-bigquery"
        )

    start_time = time.time()

    # Validate inputs
    source_path = Path(source_file)
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_file}")

    if format.lower() not in ['parquet', 'csv']:
        raise ValueError(f"Invalid format: {format}. Must be 'parquet' or 'csv'")

    if write_disposition not in ['WRITE_TRUNCATE', 'WRITE_APPEND']:
        raise ValueError(
            f"Invalid write_disposition: {write_disposition}. "
            f"Must be 'WRITE_TRUNCATE' or 'WRITE_APPEND'"
        )

    # Set up credentials
    setup_bigquery_credentials()

    # Get client config
    config = get_client_config(client_id)
    bq_service = config.get_service("bigquery")

    # Create BigQuery client
    bq_client = bigquery.Client(project=bq_service['project'])

    # Get client's dataset
    dataset = config.get_bigquery_dataset()

    # Full table ID
    table_id = f"{bq_service['project']}.{dataset}.{table_name}"

    logger.info(f"Uploading {source_file} to {table_id} ({write_disposition})")

    # Configure load job
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.PARQUET if format.lower() == 'parquet' else bigquery.SourceFormat.CSV
    )

    # For CSV, might need to configure additional options
    if format.lower() == 'csv':
        job_config.skip_leading_rows = 1  # Skip header
        job_config.autodetect = True  # Auto-detect schema

    # Upload file
    try:
        with open(source_path, "rb") as source_file_obj:
            load_job = bq_client.load_table_from_file(
                source_file_obj,
                table_id,
                job_config=job_config
            )

        # Wait for job to complete
        load_job.result()

        # Get stats
        destination_table = bq_client.get_table(table_id)
        rows_written = destination_table.num_rows
        bytes_processed = source_path.stat().st_size

        duration = time.time() - start_time

        logger.info(
            f"✅ Uploaded {rows_written} rows to {table_name} "
            f"({bytes_processed:,} bytes) in {duration:.1f}s"
        )

    except GoogleCloudError as e:
        logger.error(f"BigQuery upload failed: {e}")
        raise RuntimeError(f"Failed to upload to BigQuery: {e}") from e

    # Invalidate cache
    cache_keys_deleted = 0
    if invalidate_cache:
        cache_keys_deleted = _invalidate_cache_for_table(table_name, client_id)

    return {
        "table": table_name,
        "table_id": table_id,
        "rows_written": rows_written,
        "bytes_processed": bytes_processed,
        "duration_seconds": round(duration, 2),
        "write_disposition": write_disposition,
        "cache_invalidated": invalidate_cache,
        "cache_keys_deleted": cache_keys_deleted,
        "timestamp": datetime.now().isoformat()
    }


def delete_records(
    table_name: str,
    where_clause: str,
    client_id: str = None,
    invalidate_cache: bool = True
) -> Dict[str, Any]:
    """
    Delete records from BigQuery table with automatic cache invalidation

    Args:
        table_name: Target table name (e.g., 'transactions')
        where_clause: SQL WHERE clause (e.g., "transaction_date < '2020-01-01'")
        client_id: Client identifier (uses default if None)
        invalidate_cache: Whether to invalidate cache after delete (default: True)

    Returns:
        {
            "table": "transactions",
            "rows_deleted": 150,
            "cache_invalidated": True,
            "cache_keys_deleted": 5
        }

    Raises:
        RuntimeError: BigQuery delete failed
    """
    if not BIGQUERY_AVAILABLE:
        raise ImportError(
            "BigQuery is not available. Install with: pip install google-cloud-bigquery"
        )

    # Set up credentials
    setup_bigquery_credentials()

    # Get client config
    config = get_client_config(client_id)
    bq_service = config.get_service("bigquery")

    # Create BigQuery client
    bq_client = bigquery.Client(project=bq_service['project'])

    # Get client's dataset
    dataset = config.get_bigquery_dataset()

    # Full table ID
    table_id = f"{bq_service['project']}.{dataset}.{table_name}"

    # Build DELETE query
    delete_sql = f"DELETE FROM `{table_id}` WHERE {where_clause}"

    logger.info(f"Deleting records from {table_id}: {where_clause}")

    try:
        # Execute delete
        query_job = bq_client.query(delete_sql)
        query_job.result()

        # Get number of rows affected
        rows_deleted = query_job.num_dml_affected_rows

        logger.info(f"✅ Deleted {rows_deleted} rows from {table_name}")

    except GoogleCloudError as e:
        logger.error(f"BigQuery delete failed: {e}")
        raise RuntimeError(f"Failed to delete from BigQuery: {e}") from e

    # Invalidate cache
    cache_keys_deleted = 0
    if invalidate_cache:
        cache_keys_deleted = _invalidate_cache_for_table(table_name, client_id)

    return {
        "table": table_name,
        "table_id": table_id,
        "rows_deleted": rows_deleted,
        "cache_invalidated": invalidate_cache,
        "cache_keys_deleted": cache_keys_deleted,
        "timestamp": datetime.now().isoformat()
    }


def create_table(
    table_name: str,
    schema: List[Dict[str, str]],
    client_id: str = None
) -> Dict[str, Any]:
    """
    Create new BigQuery table

    Args:
        table_name: Table name (e.g., 'new_analytics_table')
        schema: List of field definitions, e.g.:
            [
                {"name": "isin", "type": "STRING", "mode": "REQUIRED"},
                {"name": "price", "type": "FLOAT64", "mode": "NULLABLE"},
                {"name": "date", "type": "DATE", "mode": "REQUIRED"}
            ]
        client_id: Client identifier (uses default if None)

    Returns:
        {
            "table": "new_analytics_table",
            "table_id": "project.dataset.table",
            "num_fields": 3,
            "created": True
        }

    Raises:
        RuntimeError: BigQuery table creation failed
    """
    if not BIGQUERY_AVAILABLE:
        raise ImportError(
            "BigQuery is not available. Install with: pip install google-cloud-bigquery"
        )

    # Set up credentials
    setup_bigquery_credentials()

    # Get client config
    config = get_client_config(client_id)
    bq_service = config.get_service("bigquery")

    # Create BigQuery client
    bq_client = bigquery.Client(project=bq_service['project'])

    # Get client's dataset
    dataset = config.get_bigquery_dataset()

    # Full table ID
    table_id = f"{bq_service['project']}.{dataset}.{table_name}"

    # Build schema
    bq_schema = []
    for field in schema:
        bq_schema.append(
            bigquery.SchemaField(
                name=field['name'],
                field_type=field['type'],
                mode=field.get('mode', 'NULLABLE')
            )
        )

    # Create table
    table = bigquery.Table(table_id, schema=bq_schema)

    logger.info(f"Creating table {table_id} with {len(bq_schema)} fields")

    try:
        table = bq_client.create_table(table)
        logger.info(f"✅ Created table {table_name}")

        return {
            "table": table_name,
            "table_id": table_id,
            "num_fields": len(bq_schema),
            "created": True,
            "timestamp": datetime.now().isoformat()
        }

    except GoogleCloudError as e:
        logger.error(f"BigQuery table creation failed: {e}")
        raise RuntimeError(f"Failed to create table: {e}") from e


def _invalidate_cache_for_table(table_name: str, client_id: str = None) -> int:
    """
    Invalidate cache keys related to a specific table

    Args:
        table_name: Table name that was modified
        client_id: Client ID (optional)

    Returns:
        Number of cache keys deleted
    """
    cache = get_cache_manager()

    if not cache.enabled:
        logger.debug("Cache not enabled - skipping invalidation")
        return 0

    config = get_client_config(client_id)

    # Determine cache patterns to invalidate based on table
    patterns = []

    if table_name == 'agg_analysis_data':
        # Universe data - invalidate all universe queries
        patterns = ["universe:*", f"query:*agg_analysis_data*"]
        logger.info("Invalidating universe data cache")

    elif 'transaction' in table_name.lower():
        # Transactions - invalidate for this client
        patterns = [
            f"transactions:{config.client_id}:*",
            f"query:{config.client_id}:*transaction*"
        ]
        logger.info(f"Invalidating transaction cache for {config.client_id}")

    elif 'holding' in table_name.lower():
        # Holdings - invalidate for this client
        patterns = [
            f"holdings:{config.client_id}:*",
            f"query:{config.client_id}:*holding*"
        ]
        logger.info(f"Invalidating holdings cache for {config.client_id}")

    else:
        # Generic - invalidate any queries mentioning this table
        patterns = [f"query:*{table_name}*"]
        logger.info(f"Invalidating cache for table {table_name}")

    # Invalidate all matching patterns
    total_deleted = 0
    for pattern in patterns:
        deleted = cache.invalidate(pattern)
        total_deleted += deleted

    logger.info(f"Cache invalidation complete: {total_deleted} keys deleted")

    return total_deleted


def invalidate_cache_pattern(pattern: str) -> Dict[str, Any]:
    """
    Manually invalidate cache keys matching pattern (admin tool)

    Args:
        pattern: Redis pattern (e.g., "universe:*", "holdings:client001:*")

    Returns:
        {
            "pattern": "universe:*",
            "keys_deleted": 15,
            "timestamp": "2025-11-20T15:30:00"
        }
    """
    cache = get_cache_manager()

    if not cache.enabled:
        return {
            "pattern": pattern,
            "keys_deleted": 0,
            "cache_enabled": False,
            "timestamp": datetime.now().isoformat()
        }

    keys_deleted = cache.invalidate(pattern)

    return {
        "pattern": pattern,
        "keys_deleted": keys_deleted,
        "cache_enabled": True,
        "timestamp": datetime.now().isoformat()
    }


def get_cache_stats() -> Dict[str, Any]:
    """
    Get cache statistics (admin tool)

    Returns:
        {
            "enabled": True,
            "total_keys": 142,
            "keyspace_hits": 8532,
            "keyspace_misses": 1243,
            "hit_rate_percent": 87.3,
            "memory_used": "12.5M"
        }
    """
    cache = get_cache_manager()
    return cache.get_stats()


if __name__ == "__main__":
    # Test upload functionality
    import sys
    import tempfile
    import pandas as pd

    logging.basicConfig(level=logging.INFO)

    # Create test data
    test_data = pd.DataFrame({
        'isin': ['XS1234567890', 'XS0987654321'],
        'price': [100.5, 98.25],
        'date': ['2025-11-20', '2025-11-20']
    })

    # Save to temp parquet
    with tempfile.NamedTemporaryFile(mode='w', suffix='.parquet', delete=False) as f:
        parquet_file = f.name

    test_data.to_parquet(parquet_file)

    print(f"Test parquet file: {parquet_file}")
    print("Ready to test upload_table() function")
