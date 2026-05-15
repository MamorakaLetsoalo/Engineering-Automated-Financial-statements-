# Databricks notebook source
# MAGIC %md
# MAGIC # 🎛️ Master Pipeline Orchestrator
# MAGIC **Role:** Entry point. Controls stage sequencing, dependency management,
# MAGIC retry logic, checkpoint recovery, and end-to-end run logging.
# MAGIC
# MAGIC ## Pipeline DAG
# MAGIC ```
# MAGIC ingest_financials ──┐
# MAGIC                      ├──▶ dq_validation ──▶ silver_transforms
# MAGIC ingest_macro      ──┘                              │
# MAGIC                                                    ▼
# MAGIC                                           forecasting_engine
# MAGIC                                                    │
# MAGIC                                                    ▼
# MAGIC                                        financial_statements
# MAGIC                                                    │
# MAGIC                                            ┌───────┴───────┐
# MAGIC                                            ▼               ▼
# MAGIC                                        export_csv     export_excel
# MAGIC                                            │
# MAGIC                                            ▼
# MAGIC                                     dashboard_refresh
# MAGIC ```
# MAGIC
# MAGIC ## Retry behaviour
# MAGIC - Each stage retries up to `PIPELINE["max_retries"]` times
# MAGIC - Completed stages are checkpointed — retries skip them
# MAGIC - Hard failures in DQ abort the entire pipeline

# COMMAND ----------
# MAGIC %run ../configs/project_config
# MAGIC %run ../configs/pipeline_utils
# MAGIC %run ../schemas/schemas

# COMMAND ----------
import json
import time
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
init_utils(spark)

# ── Run identity ──────────────────────────────────────────────────────────────
# Allow passing run_id from external scheduler (Databricks Workflows)
try:
    RUN_ID = dbutils.widgets.get("run_id")
except Exception:
    RUN_ID = get_run_id()

# ── Widget parameters (override defaults via Databricks Workflows UI) ─────────
try:
    FORCE_FULL_REFRESH  = dbutils.widgets.get("force_full_refresh") == "true"
    START_FROM_STAGE    = dbutils.widgets.get("start_from_stage") or None
    SCENARIO_OVERRIDE   = dbutils.widgets.get("scenario") or None
except Exception:
    FORCE_FULL_REFRESH  = False
    START_FROM_STAGE    = None
    SCENARIO_OVERRIDE   = None

logger = PipelineLogger(
    run_id=RUN_ID,
    stage="orchestrator",
    audit_path=f"{PATHS['audit']}/audit_log"
)

cp = CheckpointManager(spark, PATHS["checkpoints"], RUN_ID)

if FORCE_FULL_REFRESH:
    cp.reset()
    logger.warn("Force full refresh — all checkpoints cleared")

logger.info(
    f"Pipeline started | run_id={RUN_ID}",
    event_type="pipeline_start",
    metadata={
        "config": get_config_snapshot(),
        "force_refresh": FORCE_FULL_REFRESH,
        "start_from": START_FROM_STAGE,
    }
)

print(f"""
╔══════════════════════════════════════════════════════╗
║        FinModel Pro — Pipeline Orchestrator          ║
╠══════════════════════════════════════════════════════╣
║  Run ID   : {RUN_ID:<40}║
║  Env      : {PROJECT['environment']:<40}║
║  Company  : {PROJECT['company']:<40}║
║  Started  : {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'):<40}║
╚══════════════════════════════════════════════════════╝
""")

# COMMAND ----------
# MAGIC %md ## Stage Runner

# COMMAND ----------
class StageRunner:
    """
    Runs a notebook stage with:
    - Dependency checking (upstream stages must be done)
    - Checkpoint skip (don't rerun completed stages)
    - Retry logic (configurable max_retries)
    - Timing and logging
    """

    def __init__(self, logger, checkpoint_mgr, run_id):
        self.logger  = logger
        self.cp      = checkpoint_mgr
        self.run_id  = run_id
        self.results = {}   # stage → {status, duration, rows}

    def run(self, stage_name: str, notebook_path: str,
            depends_on: list = None,
            params: dict = None,
            max_retries: int = None,
            skip_if_done: bool = True) -> bool:
        """
        Execute a notebook stage.
        Returns True on success, False on failure (non-fatal stages).
        Raises on fatal failures.
        """
        depends_on  = depends_on or []
        params      = params or {}
        max_retries = max_retries or PIPELINE["max_retries"]

        # ── Check dependencies ────────────────────────────────────────────
        for dep in depends_on:
            if self.results.get(dep, {}).get("status") != "success":
                msg = (f"Stage '{stage_name}' skipped — "
                       f"dependency '{dep}' did not succeed")
                self.logger.warn(msg, event_type="stage_skipped")
                self.results[stage_name] = {"status": "skipped"}
                return False

        # ── Checkpoint skip ───────────────────────────────────────────────
        if skip_if_done and self.cp.is_done(stage_name):
            self.logger.info(f"Stage '{stage_name}' already done — skipping (checkpoint)",
                             event_type="stage_skipped")
            self.results[stage_name] = {"status": "success", "from_checkpoint": True}
            return True

        # ── Execute with retries ──────────────────────────────────────────
        params["run_id"] = self.run_id
        last_error = None
        start_time = time.time()

        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(
                    f"Running stage: {stage_name} (attempt {attempt}/{max_retries})",
                    event_type="stage_start"
                )

                result = dbutils.notebook.run(
                    notebook_path,
                    timeout_seconds=PIPELINE["timeout_minutes"] * 60,
                    arguments={**params, "attempt": str(attempt)}
                )

                duration = time.time() - start_time
                self.results[stage_name] = {
                    "status"  : "success",
                    "duration": round(duration, 1),
                    "result"  : result,
                }
                self.cp.mark_done(stage_name, {"duration": duration})
                self.logger.info(
                    f"Stage '{stage_name}' succeeded in {duration:.1f}s",
                    event_type="stage_complete",
                    metadata={"attempts": attempt, "duration": duration}
                )
                return True

            except Exception as e:
                last_error = e
                duration   = time.time() - start_time
                self.logger.warn(
                    f"Stage '{stage_name}' attempt {attempt} failed: {e}",
                    metadata={"attempt": attempt, "error": str(e)}
                )
                if attempt < max_retries:
                    delay = PIPELINE["retry_delay_seconds"] * attempt  # backoff
                    self.logger.info(f"Retrying in {delay}s...")
                    time.sleep(delay)

        # All retries exhausted
        self.cp.mark_failed(stage_name, str(last_error))
        self.results[stage_name] = {"status": "failed", "error": str(last_error)}
        self.logger.error(
            f"Stage '{stage_name}' failed after {max_retries} attempts",
            event_type="stage_failed",
            metadata={"error": str(last_error)}
        )
        return False


runner = StageRunner(logger, cp, RUN_ID)

# COMMAND ----------
# MAGIC %md ## Execute Pipeline

# COMMAND ----------
pipeline_start = time.time()
pipeline_failed = False

# ── Stage definitions ─────────────────────────────────────────────────────────
# Each stage maps to a notebook. depends_on ensures correct ordering.
# Fatal=True means pipeline aborts on failure; False = warn and continue.

STAGE_DEFINITIONS = [
    {
        "name"       : "01_ingest_financials",
        "notebook"   : "../ingestion/ingest_financials",
        "depends_on" : [],
        "fatal"      : True,
    },
    {
        "name"       : "02_ingest_macro",
        "notebook"   : "../ingestion/ingest_macro",
        "depends_on" : [],
        "fatal"      : True,
    },
    {
        "name"       : "03_dq_validation",
        "notebook"   : "../quality/dq_framework",
        "depends_on" : ["01_ingest_financials", "02_ingest_macro"],
        "fatal"      : True,   # Abort if DQ fails beyond threshold
    },
    {
        "name"       : "04_silver_transforms",
        "notebook"   : "../processing/silver_transforms",
        "depends_on" : ["03_dq_validation"],
        "fatal"      : True,
    },
    {
        "name"       : "05_forecasting",
        "notebook"   : "../forecasting/forecasting_engine",
        "depends_on" : ["04_silver_transforms"],
        "fatal"      : True,
    },
    {
        "name"       : "06_financial_statements",
        "notebook"   : "../statements/financial_statements",
        "depends_on" : ["05_forecasting"],
        "fatal"      : True,
    },
    {
        "name"       : "07_export",
        "notebook"   : "../statements/export_layer",
        "depends_on" : ["06_financial_statements"],
        "fatal"      : False,  # Export failure doesn't kill pipeline
    },
]

# ── Honour START_FROM_STAGE (skip earlier stages on manual rerun) ─────────────
skip_until = None
if START_FROM_STAGE:
    skip_until = START_FROM_STAGE
    logger.warn(f"Starting from stage: {START_FROM_STAGE}")

# ── Run each stage ────────────────────────────────────────────────────────────
for stage_def in STAGE_DEFINITIONS:
    sname = stage_def["name"]

    # Skip stages before START_FROM_STAGE
    if skip_until:
        if sname == skip_until:
            skip_until = None  # start running from here
        else:
            logger.info(f"Skipping {sname} (before start_from_stage)")
            runner.results[sname] = {"status": "success"}  # treat as done
            continue

    success = runner.run(
        stage_name    = sname,
        notebook_path = stage_def["notebook"],
        depends_on    = stage_def["depends_on"],
        params        = {"run_id": RUN_ID},
    )

    if not success and stage_def["fatal"]:
        pipeline_failed = True
        logger.error(
            f"Fatal stage '{sname}' failed — aborting pipeline",
            event_type="pipeline_abort"
        )
        break

# COMMAND ----------
# MAGIC %md ## Pipeline Summary

# COMMAND ----------
total_duration = time.time() - pipeline_start
status         = "FAILED" if pipeline_failed else "SUCCESS"
icon           = "❌" if pipeline_failed else "✅"

print(f"""
╔══════════════════════════════════════════════════════╗
║          Pipeline Complete — {status:<24}║
╠══════════════════════════════════════════════════════╣
║  Run ID   : {RUN_ID:<40}║
║  Duration : {f'{total_duration:.1f}s':<40}║
║  Status   : {f'{icon} {status}':<40}║
╠══════════════════════════════════════════════════════╣
║  Stage Results                                       ║
╠══════════════════════════════════════════════════════╣""")

for stage_def in STAGE_DEFINITIONS:
    sname = stage_def["name"]
    r     = runner.results.get(sname, {"status": "not_run"})
    s     = r.get("status", "not_run")
    d     = r.get("duration", "—")
    icon_s = {"success": "✅", "failed": "❌", "skipped": "⏭️ ",
               "not_run": "⬜"}.get(s, "❓")
    ckpt  = " (checkpoint)" if r.get("from_checkpoint") else ""
    print(f"║  {icon_s} {sname:<30} {str(d) + 's' if d != '—' else '—':<10}{ckpt:<12}║")

print("╚══════════════════════════════════════════════════════╝")

# Write final run status to audit log
logger.info(
    f"Pipeline {status} in {total_duration:.1f}s",
    event_type="pipeline_complete",
    metadata={
        "status"            : status,
        "duration_seconds"  : round(total_duration, 1),
        "stages"            : runner.results,
        "run_id"            : RUN_ID,
    }
)
logger.flush()

# Expose run_id for Databricks Workflows downstream tasks
try:
    dbutils.jobs.taskValues.set("run_id", RUN_ID)
    dbutils.jobs.taskValues.set("pipeline_status", status)
    dbutils.jobs.taskValues.set("pipeline_duration", str(round(total_duration, 1)))
except: pass

if pipeline_failed:
    raise RuntimeError(f"Pipeline {RUN_ID} failed — check audit log for details")
