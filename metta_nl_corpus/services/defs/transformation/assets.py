import random
from collections.abc import Mapping
from enum import StrEnum
from typing import NamedTuple

import polars as pl
from dagster import asset
from huggingface_hub.utils import tqdm
from ollama import chat
from structlog import getLogger

from metta_nl_corpus.constants import ANNOTATION_GUIDELINE_PATH, ANNOTATIONS_PATH
from metta_nl_corpus.lib.interfaces import Fn
from metta_nl_corpus.lib.pipeline_config import PipelineRunConfig
from metta_nl_corpus.services.defs.ingestion.assets import (
    DATA_VERSION,
    Annotations,
    TrainingData,
)

logger = getLogger(__name__)


class GenerationFailureReason(StrEnum):
    NO_CONTENT = "no_content"
    NO_METTA_EXPRESSION = "no_metta_expression"
    EMPTY_EXPRESSION = "empty_expression"


class GenerationResult(NamedTuple):
    expression: str | None
    failure_reason: GenerationFailureReason | None


def ollama_generate_from_template(
    text: str, additional_context: str | None = None
) -> GenerationResult:
    user_content: str = 'Turn the following statement into MeTTa expressions that represents the same meaning: "{text}". Return only valid MeTTa expressions with no comments.'.format(
        text=text
    )

    if additional_context:
        user_content = f"{user_content}\n\n{additional_context}"

    response = chat(
        model="gemma3:1b",
        messages=[
            {"role": "system", "content": ANNOTATION_GUIDELINE_PATH.read_text()},
            {
                "role": "user",
                "content": user_content,
            },
        ],
    )

    content: str | None = response.message.content

    logger.info("Generated LLM response", content=content)

    if not content:
        return GenerationResult(
            expression=None, failure_reason=GenerationFailureReason.NO_CONTENT
        )

    # Try to extract MeTTa expression from content
    try:
        # Remove outer parentheses using rsplit and lsplit
        trimmed = content.strip().lstrip("(").rstrip(")")

        if not trimmed:
            return GenerationResult(
                expression=None, failure_reason=GenerationFailureReason.EMPTY_EXPRESSION
            )
    except Exception:
        return GenerationResult(
            expression=None, failure_reason=GenerationFailureReason.NO_METTA_EXPRESSION
        )

    return GenerationResult(expression=f"({trimmed})", failure_reason=None)


def validate_expressions_are_contradictory(premise: str, hypothesis: str) -> bool:
    # TODO(seb): implement real validation logic
    return random.random() > 0.5


def generate_and_validate(
    premise: str, hypothesis: str, max_attempts: int = 3
) -> tuple[str, str] | tuple[None, None]:
    """
    Generate MeTTa expressions for premise and hypothesis, validate them,
    and retry with additional context if validation fails.

    Returns a tuple of (metta_premise, metta_hypothesis) or (None, None) if generation fails.
    """
    additional_context = None
    for attempt in range(max_attempts):
        logger.info(f"Generating MeTTa expressions (attempt {attempt})")

        metta_premise = ollama_generate_from_template(premise, additional_context)
        metta_hypothesis = ollama_generate_from_template(hypothesis, additional_context)

        if not metta_premise.expression or not metta_hypothesis.expression:
            return (None, None)

        if validate_expressions_are_contradictory(
            metta_premise.expression, metta_hypothesis.expression
        ):
            return (metta_premise.expression, metta_hypothesis.expression)

        # If validation failed, generate again with additional context
        additional_context = f"The previous result was not a contradiction as expected. Please ensure the generated MeTTa expressions represent a contradiction. Here are the previous results: {premise=} {hypothesis=} {metta_premise=} {metta_hypothesis=}"

    return (None, None)


def process_in_batches(
    rows: list[dict],
    process_fn: Fn[dict, Mapping],
    subset_size: int,
    batch_size: int,
    description: str = "Processing",
) -> list[dict]:
    """
    Process rows in batches with progress tracking.

    Args:
        rows: List of row dictionaries to process
        process_fn: Function to apply to each row
        subset_size: Maximum number of rows to process
        batch_size: Number of rows to process in each batch
        description: Description for the progress bar

    Returns:
        List of processed row dictionaries
    """
    # Limit to subset_size
    rows_to_process = rows[:subset_size]
    processed_rows = []

    # Process in batches with tqdm
    for i in tqdm(
        range(0, len(rows_to_process), batch_size),
        desc=description,
        unit="batch",
        total=(len(rows_to_process) + batch_size - 1) // batch_size,
    ):
        batch = rows_to_process[i : min(subset_size, i + batch_size)]
        batch_results = [process_fn(row) for row in batch]
        processed_rows.extend(batch_results)

    return processed_rows


@asset(required_resource_keys={"pipeline_config"})
def data_annotations(
    context,
    preprocessed_training_data: pl.DataFrame,
    cached_annotations: pl.DataFrame,
) -> pl.DataFrame:
    pipeline_config: PipelineRunConfig = context.resources.pipeline_config

    logger.info("Starting data annotation", pipeline_config=pipeline_config)

    # Get indices not in preprocessed training data
    unannotated_data_points = preprocessed_training_data.filter(
        ~pl.col(str(Annotations.index)).is_in(
            cached_annotations[str(Annotations.index)]
        )
    )

    # Extract premise and hypothesis columns into a simple dataset
    premise_hypothesis_dataset = unannotated_data_points.select(
        [
            pl.col(str(TrainingData.premise)),
            pl.col(str(TrainingData.hypothesis)),
        ]
    )

    # Apply generate_and_validate to all rows
    def process_row(row: dict) -> dict:
        premise = row[str(TrainingData.premise)]
        hypothesis = row[str(TrainingData.hypothesis)]
        metta_premise, metta_hypothesis = generate_and_validate(premise, hypothesis)
        return {
            str(Annotations.metta_premise): metta_premise,
            str(Annotations.metta_hypothesis): metta_hypothesis,
        }

    rows = premise_hypothesis_dataset.to_dicts()
    processed_rows = process_in_batches(
        rows=rows,
        process_fn=process_row,
        subset_size=pipeline_config.subset_size,
        batch_size=pipeline_config.batch_size,
        description="Generating MeTTa annotations",
    )
    generated_results = pl.DataFrame(
        processed_rows,
        schema={
            str(Annotations.metta_premise): pl.String,
            str(Annotations.metta_hypothesis): pl.String,
        },
    )

    # Add the generated columns to the annotated_data_points dataset
    annotated_data_points = (
        unannotated_data_points.head(pipeline_config.subset_size)
        .with_columns(
            [
                generated_results[str(Annotations.metta_premise)],
                generated_results[str(Annotations.metta_hypothesis)],
                pl.lit(DATA_VERSION).alias(Annotations.version),
            ]
        )
        .drop_nulls()
    )

    versioned_data = Annotations.validate(annotated_data_points)
    new_result = pl.concat([cached_annotations, versioned_data], how="align")

    new_result.write_parquet(ANNOTATIONS_PATH)

    return new_result
