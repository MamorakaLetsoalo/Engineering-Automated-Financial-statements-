"""
pipeline_utils.py
=================
Shared utilities used by every notebook:
  - PipelineLogger   : structured logging + audit trail to Delta
  - DeltaWriter      : idempotent, partitioned Delta writes
  - retry            : decorator for retryable operations
  - StageTimer       : context manager for timing stages

Import pattern in each notebook:
    %run ../configs/pipeline_utils
"""

import json
import time
import traceback
import functools
from datetime import datetime
from typing import Optional, Callable, Any

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType

# Lazy spark reference — notebooks set this before importing utils
_spark: Optional[SparkSession] = None

def init_utils(spark: SparkSession):
    """Call once at the top of each notebook: init_utils(spark)"""
    global _spark
    _spark = spark


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE LOGGER
# ─────────────────────────────────────────────────────────────────────────────

class PipelineLogger:
    """
    Structured logger that writes to:
    1. Python stdout (visible in Databricks notebook output)
    2. Delta audit log table (queryable, persistent)

    Usage:
        logger = PipelineLogger(run_id="run_20240115_143000", stage="ingestion")
        logger.info("Starting ingestion", rows_affected=1200)
        logger.warn("Null values detected", details={"column": "revenue", "count": 3})
        logger.error("Schema mismatch", error=e)
    """

    def __init__(self, run_id: str, stage: str, audit_path: str):
        self.run_id     = run_id
        self.stage      = stage
        self.audit_path = audit_path
        self._buffer    = []   # batch writes to Delta

    def _log(self, severity: str, message: str,
             event_type: str = "stage_event",
             table_name: Optional[str] = None,
             rows_affected: Optional[int] = None,
             metadata: Optional[dict] = None):

        ts = datetime.utcnow()
        entry = {
            "run_id"        : self.run_id,
            "event_type"    : event_type,
            "stage"         : self.stage,
            "table_name"    : table_name,
            "rows_affected" : rows_affected,
            "message"       : message,
            "severity"      : severity,
            "metadata_json" : json.dumps(metadata) if metadata else None,
            "recorded_at"   : ts,
        }

        # Stdout
        prefix = {"INFO": "✅", "WARN": "⚠️ ", "ERROR": "❌"}[severity]
        print(f"{prefix} [{ts.strftime('%H:%M:%S')}] [{self.stage}] {message}"
              + (f" | rows={rows_affected}" if rows_affected else "")
              + (f" | {json.dumps(metadata)}" if metadata else ""))

        self._buffer.append(entry)

    def info(self, message: str, **kwargs):  self._log("INFO",  message, **kwargs)
    def warn(self, message: str, **kwargs):  self._log("WARN",  message, **kwargs)
    def error(self, message: str, **kwargs): self._log("ERROR", message, **kwargs)

    def flush(self):
        """Write buffered log entries to Delta audit table."""
        if not self._buffer or _spark is None:
            return
        sdf = _spark.createDataFrame(self._buffer)
        (sdf.write
             .format("delta")
             .mode("append")
             .save(self.audit_path))
        self._buffer = []

    def __del__(self):
        try: self.flush()
        except: pass


# ─────────────────────────────────────────────────────────────────────────────
# STAGE TIMER
# ─────────────────────────────────────────────────────────────────────────────

class StageTimer:
    """
    Context manager that times a pipeline stage and logs duration.

    Usage:
        with StageTimer("silver_transforms", logger) as t:
            # ... do work ...
            t.set_rows(row_count)
    """
    def __init__(self, stage_name: str, logger: PipelineLogger,
                 run_log_path: str):
        self.stage_name   = stage_name
        self.logger       = logger
        self.run_log_path = run_log_path
        self._rows        = None
        self._start       = None

    def set_rows(self, n: int): self._rows = n

    def __enter__(self):
        self._start = time.time()
        self.logger.info(f"Stage started: {self.stage_name}",
                         event_type="stage_start")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self._start
        status   = "failed" if exc_type else "success"
        error_msg = str(exc_val) if exc_val else None

        self.logger.info(
            f"Stage {status}: {self.stage_name} "
            f"({duration:.1f}s{f', {self._rows:,} rows' if self._rows else ''})",
            event_type="stage_complete",
            metadata={"duration_seconds": round(duration, 2),
                      "status": status,
                      "error": error_msg}
        )

        if _spark and self.run_log_path:
            log_row = [{
                "run_id"          : self.logger.run_id,
                "stage"           : self.stage_name,
                "status"          : status,
                "started_at"      : datetime.utcfromtimestamp(self._start),
                "completed_at"    : datetime.utcnow(),
                "duration_seconds": round(duration, 2),
                "rows_processed"  : self._rows,
                "rows_failed"     : None,
                "error_message"   : error_msg,
                "config_snapshot" : None,
            }]
            _spark.createDataFrame(log_row).write \
                  .format("delta").mode("append").save(self.run_log_path)

        self.logger.flush()
        return False  # re-raise exceptions


# ─────────────────────────────────────────────────────────────────────────────
# RETRY DECORATOR
# ─────────────────────────────────────────────────────────────────────────────

def retry(max_attempts: int = 3, delay_seconds: int = 30,
          exceptions=(Exception,)):
    """
    Decorator: retries a function up to max_attempts times.
    Useful for API calls (FRED) and Delta writes under contention.

    Usage:
        @retry(max_attempts=3, delay_seconds=15)
        def fetch_fred_data(...): ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt < max_attempts:
                        print(f"⚠️  Attempt {attempt}/{max_attempts} failed: {e}. "
                              f"Retrying in {delay_seconds}s...")
                        time.sleep(delay_seconds)
                    else:
                        print(f"❌ All {max_attempts} attempts failed for {func.__name__}")
            raise last_exc
        return wrapper
    return decorator


# ─────────────────────────────────────────────────────────────────────────────
# DELTA WRITER
# ─────────────────────────────────────────────────────────────────────────────

class DeltaWriter:
    """
    Idempotent, partitioned Delta table writer.

    Supports:
    - overwrite (full refresh)
    - append
    - merge  (upsert on key columns — avoids duplicate records on reruns)
    - incremental (only write new partitions)

    Design decision: merge mode uses Delta MERGE INTO (not overwrite) so
    reruns are idempotent — critical for scheduled pipelines that may retry.
    """

    def __init__(self, spark: SparkSession, path: str,
                 table_name: str, partition_cols: list[str] = None):
        self.spark          = spark
        self.path           = path
        self.table_name     = table_name
        self.partition_cols = partition_cols or []

    def write(self, sdf: DataFrame, mode: str = "overwrite",
              merge_keys: list[str] = None,
              schema: StructType = None) -> int:
        """
        Write DataFrame to Delta.

        Args:
            sdf        : Spark DataFrame to write
            mode       : overwrite | append | merge
            merge_keys : columns to match on for merge/upsert
            schema     : enforce schema (raises on mismatch)
        Returns:
            Row count written
        """
        if schema:
            sdf = self.spark.createDataFrame(sdf.toPandas(), schema=schema)

        count = sdf.count()

        writer = (sdf.write
                     .format("delta")
                     .option("overwriteSchema", "true"))

        if self.partition_cols:
            writer = writer.partitionBy(*self.partition_cols)

        if mode == "merge" and merge_keys:
            self._merge(sdf, merge_keys)
        elif mode == "incremental":
            self._incremental_write(sdf)
        else:
            writer.mode(mode).save(self.path)

        # Register as table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}
            USING DELTA LOCATION '{self.path}'
        """)

        # Optimize + Z-order for query performance on large tables
        if self.partition_cols and count > 10_000:
            try:
                z_col = self.partition_cols[0]
                self.spark.sql(f"OPTIMIZE delta.`{self.path}` ZORDER BY ({z_col})")
            except Exception:
                pass  # Z-order non-critical

        return count

    def _merge(self, sdf: DataFrame, merge_keys: list[str]):
        """MERGE INTO — upsert on key columns."""
        from delta.tables import DeltaTable
        if DeltaTable.isDeltaTable(self.spark, self.path):
            dt = DeltaTable.forPath(self.spark, self.path)
            condition = " AND ".join(
                [f"target.{k} = source.{k}" for k in merge_keys]
            )
            dt.alias("target").merge(
                sdf.alias("source"), condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            sdf.write.format("delta").mode("overwrite").save(self.path)

    def _incremental_write(self, sdf: DataFrame):
        """Only write partitions not already present in target."""
        if not self.partition_cols:
            raise ValueError("Incremental write requires partition_cols")

        from delta.tables import DeltaTable
        if not DeltaTable.isDeltaTable(self.spark, self.path):
            sdf.write.format("delta") \
               .partitionBy(*self.partition_cols) \
               .mode("overwrite").save(self.path)
            return

        # Get existing partition values
        existing = (self.spark.read.format("delta").load(self.path)
                        .select(*self.partition_cols).distinct())
        new_data = sdf.join(existing, on=self.partition_cols, how="left_anti")
        new_count = new_data.count()
        if new_count > 0:
            new_data.write.format("delta") \
                    .partitionBy(*self.partition_cols) \
                    .mode("append").save(self.path)
            print(f"   Incremental: wrote {new_count:,} new rows")
        else:
            print("   Incremental: no new partitions to write — skipped")


# ─────────────────────────────────────────────────────────────────────────────
# CHECKPOINT MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class CheckpointManager:
    """
    Lightweight run-state tracker stored as JSON in DBFS.
    Allows pipeline to skip completed stages on retry.

    Usage:
        cp = CheckpointManager(spark, "/FileStore/.../checkpoints", run_id)
        if not cp.is_done("silver_transforms"):
            do_transforms()
            cp.mark_done("silver_transforms")
    """

    def __init__(self, spark: SparkSession, path: str, run_id: str):
        self.spark   = spark
        self.path    = path
        self.run_id  = run_id
        self._state  = self._load()

    def _load(self) -> dict:
        local = f"/dbfs{self.path}/{self.run_id}.json"
        try:
            with open(local) as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def _save(self):
        import os
        local = f"/dbfs{self.path}/{self.run_id}.json"
        os.makedirs(os.path.dirname(local), exist_ok=True)
        with open(local, "w") as f:
            json.dump(self._state, f, default=str)

    def is_done(self, stage: str) -> bool:
        return self._state.get(stage, {}).get("status") == "success"

    def mark_done(self, stage: str, metadata: dict = None):
        self._state[stage] = {
            "status"      : "success",
            "completed_at": datetime.utcnow().isoformat(),
            "metadata"    : metadata or {},
        }
        self._save()

    def mark_failed(self, stage: str, error: str):
        self._state[stage] = {
            "status"    : "failed",
            "failed_at" : datetime.utcnow().isoformat(),
            "error"     : error,
        }
        self._save()

    def reset(self, stage: str = None):
        """Reset one or all stages (force rerun)."""
        if stage:
            self._state.pop(stage, None)
        else:
            self._state = {}
        self._save()
