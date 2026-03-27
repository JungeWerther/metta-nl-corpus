"""Main CLI entry point for running the annotation pipeline."""

import asyncio
from pathlib import Path

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


@cli.command()
@click.option(
    "--model",
    default="openai:gpt-5-nano",
    help="Model to use for annotation generation",
)
@click.option(
    "--batch-size",
    default=25,
    type=int,
    help="Pairs per batch (concurrent within batch)",
)
@click.option(
    "--num-batches",
    default=20,
    type=int,
    help="Number of sequential batches",
)
@click.option(
    "--offset",
    default=5500,
    type=int,
    help="Starting row index in SNLI dataset",
)
@click.option(
    "--label",
    default=None,
    type=str,
    help="Optional label filter (entailment, contradiction, neutral)",
)
def annotate(
    model: str,
    batch_size: int,
    num_batches: int,
    offset: int,
    label: str | None,
):
    """Lightweight batch annotation — no Dagster, no subprocess validation."""
    asyncio.run(
        _run_annotate(
            model=model,
            batch_size=batch_size,
            num_batches=num_batches,
            offset=offset,
            label=label,
        )
    )


async def _run_annotate(
    model: str,
    batch_size: int,
    num_batches: int,
    offset: int,
    label: str | None,
):
    from metta_nl_corpus.constants import ANNOTATION_GUIDELINE_PATH, ANNOTATIONS_DB_PATH
    from metta_nl_corpus.lib.data_source import yield_unannotated_pairs
    from metta_nl_corpus.lib.storage import AnnotationStore
    from metta_nl_corpus.models import RelationKind
    from metta_nl_corpus.services.defs.transformation.assets import (
        _create_metta_agent,
        generate_and_store_lightweight,
    )

    store = AnnotationStore(ANNOTATIONS_DB_PATH)
    total_limit = batch_size * num_batches

    logger.info(
        "Fetching unannotated pairs",
        limit=total_limit,
        offset=offset,
        label=label,
    )
    pairs = yield_unannotated_pairs(
        store, limit=total_limit, offset=offset, label=label
    )
    logger.info("Found unannotated pairs", count=len(pairs))

    if not pairs:
        logger.info("No unannotated pairs found — nothing to do.")
        return

    system_prompt = ANNOTATION_GUIDELINE_PATH.read_text()
    agent = _create_metta_agent(system_prompt, model)

    total_input_tokens = 0
    total_output_tokens = 0
    total_stored = 0
    total_valid = 0

    for batch_idx in range(num_batches):
        batch_start = batch_idx * batch_size
        batch_end = min(batch_start + batch_size, len(pairs))
        batch = pairs[batch_start:batch_end]

        if not batch:
            break

        logger.info(
            "Processing batch",
            batch=batch_idx + 1,
            num_batches=num_batches,
            size=len(batch),
        )

        tasks = [
            generate_and_store_lightweight(
                agent=agent,
                premise=pair.premise,
                hypothesis=pair.hypothesis,
                label=RelationKind(pair.label),
                snli_index=pair.snli_index,
                annotation_model=model,
                system_prompt=system_prompt,
            )
            for pair in batch
        ]
        results = await asyncio.gather(*tasks)

        batch_stored = 0
        batch_valid = 0
        for row in results:
            if "error" in row:
                logger.warning("Generation failed", error=row["error"])
                continue
            store.insert_annotation(row)
            batch_stored += 1
            if row.get("is_valid"):
                batch_valid += 1
            total_input_tokens += row.get("input_tokens") or 0
            total_output_tokens += row.get("output_tokens") or 0

        total_stored += batch_stored
        total_valid += batch_valid

        logger.info(
            "Batch complete",
            batch=batch_idx + 1,
            stored=batch_stored,
            valid=batch_valid,
        )

    # Cost estimate
    _MODEL_PRICING = {
        "openai:gpt-4o-mini": (0.15, 0.60),
        "openai:gpt-4o": (2.50, 10.00),
        "openai:gpt-5-nano": (0.10, 0.40),
    }
    pricing = _MODEL_PRICING.get(model)
    cost_str = "unknown"
    if pricing and (total_input_tokens or total_output_tokens):
        input_rate, output_rate = pricing
        cost_usd = (
            total_input_tokens * input_rate + total_output_tokens * output_rate
        ) / 1_000_000
        cost_str = f"${cost_usd:.4f}"

    logger.info(
        "Annotation run complete",
        model=model,
        total_stored=total_stored,
        total_valid=total_valid,
        input_tokens=total_input_tokens,
        output_tokens=total_output_tokens,
        estimated_cost=cost_str,
    )


@cli.command(name="push-dataset")
@click.option(
    "--repo-id",
    default="JungeWerther/metta-nl-corpus",
    help="HuggingFace dataset repository ID",
)
@click.option(
    "--table",
    default="annotations",
    type=click.Choice(["annotations", "validations"]),
    help="Which table to export",
)
@click.option(
    "--private/--public",
    default=False,
    help="Whether the dataset repo should be private",
)
def push_dataset(repo_id: str, table: str, private: bool):
    """Export annotations from SQLite and push to HuggingFace Hub."""
    from tempfile import TemporaryDirectory

    from huggingface_hub import HfApi

    from metta_nl_corpus.constants import ANNOTATIONS_DB_PATH
    from metta_nl_corpus.lib.storage import AnnotationStore

    store = AnnotationStore(ANNOTATIONS_DB_PATH)
    api = HfApi()

    with TemporaryDirectory() as tmpdir:
        parquet_path = Path(tmpdir) / f"{table}.parquet"
        row_count = store.export_parquet(parquet_path, table=table)

        if row_count == 0:
            logger.error("No rows to export", table=table)
            return

        api.create_repo(repo_id, repo_type="dataset", private=private, exist_ok=True)
        api.upload_file(
            path_or_fileobj=str(parquet_path),
            path_in_repo=f"data/{table}.parquet",
            repo_id=repo_id,
            repo_type="dataset",
        )
        logger.info(
            "Pushed to HuggingFace Hub",
            repo_id=repo_id,
            table=table,
            rows=row_count,
        )


if __name__ == "__main__":
    cli()
