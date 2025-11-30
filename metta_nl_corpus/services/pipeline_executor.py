"""Service for executing Dagster pipelines with static assets."""

import asyncio
from enum import StrEnum
from typing import NamedTuple

# Count annotations
import polars as pl
from dagster import (
    DagsterInstance,
    materialize,
)
from structlog import get_logger

from metta_nl_corpus.lib.pipeline_config import PipelineRunConfig
from metta_nl_corpus.services.defs.ingestion.assets import (
    cached_annotations,
    preprocessed_training_data,
    raw_training_data,
)
from metta_nl_corpus.services.defs.transformation.assets import data_annotations

logger = get_logger(__name__)


class ExecutionStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    ERROR = "error"
    COMPLETED = "completed"
    NOT_FOUND = "not_found"


class ExecutionResult(NamedTuple):
    status: ExecutionStatus
    cache_key: str
    annotations_path: str | None = None
    annotations_count: int | None = None
    dataset: str | None = None
    model: str | None = None
    error: str | None = None
    message: str | None = None


class PipelineExecutor:
    """Executes Dagster pipelines with static assets."""

    def __init__(self):
        self.instance = DagsterInstance.get()

    async def execute_pipeline(
        self,
        pipeline_config: PipelineRunConfig,
    ) -> ExecutionResult:
        """
        Execute the annotation pipeline.

        Args:
            pipeline_config: Configuration for the pipeline run

        Returns:
            ExecutionResult with execution results and metadata
        """
        cache_key: str = pipeline_config.cache_key

        logger.info(
            "Starting pipeline execution",
            dataset=pipeline_config.dataset_config.hf_id,
            model=pipeline_config.model_name,
            version=pipeline_config.version,
            cache_key=cache_key,
        )

        # Collect all assets
        assets = [
            raw_training_data,
            cached_annotations,
            preprocessed_training_data,
            data_annotations,
        ]

        # Execute the pipeline
        try:
            logger.info("Materializing pipeline assets")

            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: materialize(
                    assets,
                    instance=self.instance,
                    raise_on_error=False,
                    resources={"pipeline_config": pipeline_config},
                ),
            )

            if result.success:
                annotations_count: int
                try:
                    df = pl.read_parquet(pipeline_config.annotations_path)
                    annotations_count = len(df)
                except Exception:
                    annotations_count = 0

                logger.info(
                    "Pipeline execution completed successfully",
                    cache_key=cache_key,
                    annotations_count=annotations_count,
                )
                return ExecutionResult(
                    status=ExecutionStatus.SUCCESS,
                    cache_key=cache_key,
                    annotations_path=str(pipeline_config.annotations_path),
                    annotations_count=annotations_count,
                    dataset=pipeline_config.dataset_config.hf_id,
                    model=pipeline_config.model_name,
                )
            else:
                error_msg: str = "Pipeline execution failed. Check logs for details."
                logger.error("Pipeline execution failed", cache_key=cache_key)
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    cache_key=cache_key,
                    error=error_msg,
                )

        except Exception as e:
            error_msg: str = f"Error during pipeline execution: {str(e)}"
            logger.exception("Error during pipeline execution")
            return ExecutionResult(
                status=ExecutionStatus.ERROR,
                cache_key=cache_key,
                error=str(e),
            )
