"""
KICKZ EMPIRE — ELT Pipeline Orchestrator
==========================================
Main entry point for the ELT pipeline.

Executes sequentially:
    1. Extract  → Load CSV files into Bronze (raw, as-is)
    2. Transform → Clean Bronze → Silver (conformed)
    3. Gold      → Aggregate Silver → Gold (business-ready)

Usage:
    python pipeline.py                  # Run the full pipeline
    python pipeline.py --step extract   # Run extraction only
    python pipeline.py --step transform # Run transformation only
    python pipeline.py --step gold      # Run Gold layer only
"""

import argparse
import time
from datetime import datetime, timezone

from src.extract import extract_all
from src.transform import transform_all
from src.gold import create_gold_layer

# TODO (TP4): Import the monitoring classes
from src.monitoring import PipelineReport, StepMetrics


def run_pipeline(step: str = "all"):
    print("=" * 60)
    print("  🏪 KICKZ EMPIRE — ELT Pipeline")
    print("=" * 60)

    report = PipelineReport()
    t0 = time.time()

    
    if step in ("all", "extract"):
        step_metrics = StepMetrics(step_name="extract")
        step_metrics.status = "running"
        step_metrics.start_time = datetime.now(timezone.utc).isoformat()

        try:
            results = extract_all()
            step_metrics.status = "success"
            step_metrics.rows_processed = sum(len(df) for df in results.values())
            step_metrics.tables_created = list(results.keys())

        except Exception as e:
            step_metrics.status = "failed"
            step_metrics.errors.append(str(e))
            raise 

        finally:
            step_metrics.end_time = datetime.now(timezone.utc).isoformat()
            step_metrics.duration_seconds = round(time.time() - t0, 2)
            report.add_step(step_metrics)

    
    if step in ("all", "transform"):
        step_metrics = StepMetrics(step_name="transform")
        step_metrics.status = "running"
        step_metrics.start_time = datetime.now(timezone.utc).isoformat()

        try:
            results = transform_all()
            step_metrics.status = "success"

        except Exception as e:
            step_metrics.status = "failed"
            step_metrics.errors.append(str(e))
            raise

        finally:
            step_metrics.end_time = datetime.now(timezone.utc).isoformat()
            step_metrics.duration_seconds = round(time.time() - t0, 2)
            report.add_step(step_metrics)

    
    if step in ("all", "gold"):
        step_metrics = StepMetrics(step_name="gold")
        step_metrics.status = "running"
        step_metrics.start_time = datetime.now(timezone.utc).isoformat()

        try:
            create_gold_layer()
            step_metrics.status = "success"

        except Exception as e:
            step_metrics.status = "failed"
            step_metrics.errors.append(str(e))
            raise

        finally:
            step_metrics.end_time = datetime.now(timezone.utc).isoformat()
            step_metrics.duration_seconds = round(time.time() - t0, 2)
            report.add_step(step_metrics)

    elapsed = time.time() - t0

    print(f"\n{'=' * 60}")
    print(f"  ✅ Pipeline completed in {elapsed:.1f}s")
    print(f"{'=' * 60}")

    # ✅ Correct indentation
    report.save("pipeline_report.json")
    print("  📄 Report saved to pipeline_report.json")