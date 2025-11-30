"""Main CLI entry point for running the annotation pipeline."""

import asyncio

import click
from structlog import get_logger

from metta_nl_corpus.lib.pipeline_config import DatasetConfig, PipelineRunConfig
from metta_nl_corpus.services.pipeline_executor import (
    ExecutionResult,
    ExecutionStatus,
    PipelineExecutor,
)

logger = get_logger(__name__)


@click.group()
def cli():
    """MeTTa NL Corpus pipeline management CLI."""
    pass


@cli.command()
@click.option(
    "--hf-id",
    default="squad",
    help="HuggingFace dataset ID",
)
@click.option(
    "--filename",
    default="train.parquet",
    help="Filename within the dataset repository",
)
@click.option(
    "--split",
    default="train",
    help="Dataset split",
)
@click.option(
    "--model-name",
    default="example-model",
    help="Model name for the pipeline run",
)
@click.option(
    "--version",
    default="v1",
    help="Version of the pipeline run",
)
@click.option(
    "--subset-size",
    default=10,
    type=int,
    help="Number of samples to process",
)
@click.option(
    "--batch-size",
    default=10,
    type=int,
    help="Batch size for processing",
)
@click.option(
    "--annotation-model",
    default="gemma3:1b",
    help="Model to use for annotation generation",
)
def run(
    hf_id: str,
    filename: str,
    split: str,
    model_name: str,
    version: str,
    subset_size: int,
    batch_size: int,
    annotation_model: str,
):
    """Run the annotation pipeline with specified configuration."""
    asyncio.run(
        _run_pipeline(
            hf_id=hf_id,
            filename=filename,
            split=split,
            model_name=model_name,
            version=version,
            subset_size=subset_size,
            batch_size=batch_size,
            annotation_model=annotation_model,
        )
    )


async def _run_pipeline(
    hf_id: str,
    filename: str,
    split: str,
    model_name: str,
    version: str,
    subset_size: int,
    batch_size: int,
    annotation_model: str,
):
    """Run the annotation pipeline with example configuration."""
    # Configure your dataset
    dataset_config = DatasetConfig(
        hf_id=hf_id,
        filename=filename,
        split=split,
        repo_type="dataset",
    )

    # Configure the pipeline run
    pipeline_config = PipelineRunConfig(
        dataset_config=dataset_config,
        model_name=model_name,
        version=version,
        subset_size=subset_size,
        batch_size=batch_size,
        annotation_model=annotation_model,
    )

    logger.info(
        "Starting pipeline execution",
        dataset=pipeline_config.dataset_config.hf_id,
        model=pipeline_config.annotation_model,
        subset_size=pipeline_config.subset_size,
        batch_size=pipeline_config.batch_size,
        cache_key=pipeline_config.cache_key,
    )

    # Execute the pipeline
    executor: PipelineExecutor = PipelineExecutor()
    result: ExecutionResult = await executor.execute_pipeline(pipeline_config)

    if result.status == ExecutionStatus.SUCCESS:
        logger.info(
            "Pipeline execution completed",
            status=result.status,
            annotations_path=result.annotations_path,
            annotations_count=result.annotations_count,
        )
    else:
        logger.error(
            "Pipeline execution failed",
            status=result.status,
            error=result.error,
        )


if __name__ == "__main__":
    cli()
