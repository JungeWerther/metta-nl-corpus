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
    default="openai:gpt-5-nano",
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


@cli.command()
@click.option(
    "--hf-id",
    default="JungeWerther/metta-nl-corpus-bronze-0.1",
    help="HuggingFace dataset ID for the bronze dataset",
)
@click.option(
    "--filename",
    default="annotations.parquet",
    help="Filename within the dataset repository",
)
def clean(hf_id: str, filename: str):
    """Clean and re-validate a bronze dataset."""
    asyncio.run(
        _run_clean_pipeline(
            hf_id=hf_id,
            filename=filename,
        )
    )


async def _run_clean_pipeline(
    hf_id: str,
    filename: str,
):
    """Run the cleaning pipeline."""
    logger.info(
        "Starting clean pipeline",
        hf_id=hf_id,
        filename=filename,
    )

    executor: PipelineExecutor = PipelineExecutor()
    result: ExecutionResult = await executor.execute_clean_pipeline(
        hf_id=hf_id,
        filename=filename,
    )

    if result.status == ExecutionStatus.SUCCESS:
        logger.info(
            "Clean pipeline completed",
            status=result.status,
            annotations_path=result.annotations_path,
            annotations_count=result.annotations_count,
        )
    else:
        logger.error(
            "Clean pipeline failed",
            status=result.status,
            error=result.error,
        )


@cli.command()
@click.option("--port", default=8090, type=int, help="Port for the HTTP API server")
@click.option("--host", default="0.0.0.0", help="Host to bind to")
def serve(port: int, host: str):
    """Start the HTTP annotation API server."""
    import uvicorn

    from metta_nl_corpus.http_server import create_app

    app = create_app()
    uvicorn.run(app, host=host, port=port)


@cli.command(name="mcp")
def mcp_server():
    """Start the MCP server (stdio transport)."""
    from metta_nl_corpus.mcp_server import mcp as mcp_app

    mcp_app.run()


if __name__ == "__main__":
    cli()
