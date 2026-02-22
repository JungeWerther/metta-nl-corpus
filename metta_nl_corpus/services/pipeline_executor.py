"""Service for executing Dagster pipelines with static assets."""

import asyncio
import os
from enum import StrEnum
from typing import NamedTuple

# Count annotations
import polars as pl
from dagster import (
    DagsterInstance,
    materialize,
)
from structlog import get_logger

from metta_nl_corpus.constants import ANNOTATIONS_PATH
from metta_nl_corpus.lib.pipeline_config import PipelineRunConfig
from metta_nl_corpus.constants import CLEANED_ANNOTATIONS_PATH
from metta_nl_corpus.services.defs.cleaning.assets import (
    bronze_dataset,
    cleaned_annotations,
)
from metta_nl_corpus.services.defs.ingestion.assets import (
    cached_annotations,
    cached_validations,
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
        if os.getenv("DAGSTER_HOME"):
            self.instance = DagsterInstance.get()
        else:
            self.instance = DagsterInstance.ephemeral()

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
            cached_validations,
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
                    df = pl.read_parquet(ANNOTATIONS_PATH)
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
                    annotations_path=str(ANNOTATIONS_PATH),
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

    async def execute_clean_pipeline(
        self,
        hf_id: str = "JungeWerther/metta-nl-corpus-bronze-0.1",
        filename: str = "annotations.parquet",
        keep_invalid: bool = False,
    ) -> ExecutionResult:
        """Execute the cleaning pipeline on a bronze dataset."""
        cache_key = f"clean_{hf_id}".replace("/", "_")

        logger.info(
            "Starting clean pipeline execution",
            hf_id=hf_id,
            filename=filename,
            keep_invalid=keep_invalid,
        )

        assets = [bronze_dataset, cleaned_annotations]

        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: materialize(
                    assets,
                    instance=self.instance,
                    raise_on_error=False,
                    run_config={
                        "ops": {
                            "bronze_dataset": {
                                "config": {
                                    "hf_id": hf_id,
                                    "filename": filename,
                                    "keep_invalid": keep_invalid,
                                }
                            },
                            "cleaned_annotations": {
                                "config": {
                                    "hf_id": hf_id,
                                    "filename": filename,
                                    "keep_invalid": keep_invalid,
                                }
                            },
                        }
                    },
                ),
            )

            if result.success:
                annotations_count: int = 0
                try:
                    df = pl.read_parquet(CLEANED_ANNOTATIONS_PATH)
                    annotations_count = len(df)
                except Exception:
                    pass

                logger.info(
                    "Clean pipeline completed successfully",
                    annotations_count=annotations_count,
                )
                return ExecutionResult(
                    status=ExecutionStatus.SUCCESS,
                    cache_key=cache_key,
                    annotations_path=str(CLEANED_ANNOTATIONS_PATH),
                    annotations_count=annotations_count,
                    dataset=hf_id,
                )
            else:
                logger.error("Clean pipeline failed")
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    cache_key=cache_key,
                    error="Clean pipeline failed. Check logs for details.",
                )

        except Exception as e:
            logger.exception("Error during clean pipeline execution")
            return ExecutionResult(
                status=ExecutionStatus.ERROR,
                cache_key=cache_key,
                error=str(e),
            )
