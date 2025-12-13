from collections.abc import Mapping
from enum import StrEnum
from typing import NamedTuple, Optional

import polars as pl
from dagster import asset
from dotenv import load_dotenv
from huggingface_hub.utils import tqdm
from hyperon import MeTTa
from ollama import chat
from openai import OpenAI
from structlog import getLogger

from metta_nl_corpus.constants import (
    ANNOTATION_GUIDELINE_PATH,
    ANNOTATIONS_PATH,
    PROJECT_ROOT,
)
from metta_nl_corpus.lib.interfaces import Fn
from metta_nl_corpus.lib.pipeline_config import PipelineRunConfig
from metta_nl_corpus.services.defs.ingestion.assets import (
    DATA_VERSION,
    Annotations,
    TrainingData,
    RelationKind,
)

logger = getLogger(__name__)

load_dotenv(".env.local")


class GenerationFailureReason(StrEnum):
    NO_CONTENT = "no_content"
    EMPTY_EXPRESSION = "empty_expression"


class GenerationResult(NamedTuple):
    expression: str | None
    failure_reason: GenerationFailureReason | None


def parse_metta_expression(expression: str) -> str:
    expression_lines = expression.splitlines()
    if len(expression_lines) >= 1:
        if "```" in expression_lines[0]:
            expression_lines = expression_lines[1:]
        if "```" in expression_lines[-1]:
            expression_lines = expression_lines[:-1]
        return "\n".join(expression_lines).strip()
    else:
        return expression


def extract_metta_expression(content: str | None) -> GenerationResult:
    if not content:
        return GenerationResult(
            expression=None, failure_reason=GenerationFailureReason.NO_CONTENT
        )

    trimmed = parse_metta_expression(content)
    if not trimmed:
        return GenerationResult(
            expression=None, failure_reason=GenerationFailureReason.EMPTY_EXPRESSION
        )

    return GenerationResult(expression=trimmed, failure_reason=None)


def openai_generate_from_template(
    text: str, model: str, additional_context: str | None = None
) -> GenerationResult:
    user_content: str = 'Turn the following statement into MeTTa expressions that represents the same meaning: "{text}". Return only valid MeTTa expressions with no comments.'.format(
        text=text
    )

    if additional_context:
        user_content = f"{user_content}\n\n{additional_context}"

    try:
        client = OpenAI()  # Uses OPENAI_API_KEY env var
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": ANNOTATION_GUIDELINE_PATH.read_text()},
                {"role": "user", "content": user_content},
            ],
        )
        content = response.choices[0].message.content
    except Exception as e:
        logger.error("OpenAI generation failed", error=str(e))
        return GenerationResult(
            expression=None, failure_reason=GenerationFailureReason.NO_CONTENT
        )

    logger.info("Generated LLM response", content=content)
    return extract_metta_expression(content)


def ollama_generate_from_template(
    text: str, model: str, additional_context: str | None = None
) -> GenerationResult:
    user_content: str = 'Turn the following statement into MeTTa expressions that represents the same meaning: "{text}". Return only valid MeTTa expressions with no comments.'.format(
        text=text
    )

    if additional_context:
        user_content = f"{user_content}\n\n{additional_context}"

    response = chat(
        model=model,
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
    return extract_metta_expression(content)


def validate_expressions_are_contradictory(
    metta_premise: Optional[str], metta_hypothesis: Optional[str]
) -> bool:
    if not metta_premise or not metta_hypothesis:
        return False

    try:
        runner = MeTTa()

        # Load background knowledge
        contradictions_path = (
            PROJECT_ROOT / "metta_nl_corpus/services/spaces/contradictions.metta"
        )

        # Read the file content
        with open(contradictions_path, "r") as f:
            metta_code = f.read()

        runner.run(metta_code)

        # Run premise and hypothesis
        if metta_premise:
            runner.run(metta_premise)
        if metta_hypothesis:
            runner.run(metta_hypothesis)

        # Check for intersection between truth and falsity spaces
        result = runner.run("!(match &truth $x (match &falsity $x True))")
        logger.info("MeTTa validation result", result=result)

        # result is a list of results (atoms)
        if result and len(result) > 0 and len(result[0]) > 0:
            logger.info(
                "MeTTa expressions are contradictory",
            )
            return True

    except Exception as e:
        logger.warning("MeTTa validation failed", error=str(e))
        return False

    logger.info(
        "MeTTa expressions are not contradictory",
    )
    return False


def generate_and_validate(
    premise: str,
    hypothesis: str,
    label: str,
    annotation_model: str,
    max_attempts: int = 3,
) -> tuple[str | None, str | None, bool]:
    """
    Generate MeTTa expressions for premise and hypothesis, validate them,
    and retry with additional context if validation fails.

    Returns:
        tuple[str | None, str | None, bool]: (metta_premise, metta_hypothesis, is_valid)
    """
    additional_context = None
    last_metta_premise = None
    last_metta_hypothesis = None

    is_openai = annotation_model.startswith(("gpt", "o1"))
    generate_fn = (
        openai_generate_from_template if is_openai else ollama_generate_from_template
    )

    for attempt in range(max_attempts):
        logger.info(
            f"Generating MeTTa expressions (attempt {attempt})", model=annotation_model
        )

        metta_premise_result = generate_fn(
            premise, annotation_model, additional_context
        )
        if not metta_premise_result.expression:
            logger.error(
                "Failed to generate MeTTa expression for premise",
                premise=premise,
                error=metta_premise_result.failure_reason,
            )
            continue

        metta_hypothesis_context = f"""
        Previously we processed the premise: {premise=} and generated the following MeTTa expression: {metta_premise_result.expression}.
        Now we need to process the hypothesis: {hypothesis=} and generate a MeTTa expression that can be validated as a contradiction or
        not a contradiction with the previously generated MeTTa expression.
        """
        if additional_context:
            metta_hypothesis_context += f"\n\n{additional_context}"
        metta_hypothesis_result = generate_fn(
            hypothesis, annotation_model, metta_hypothesis_context
        )

        last_metta_premise = metta_premise_result.expression
        last_metta_hypothesis = metta_hypothesis_result.expression

        logger.info(
            "Generated MeTTa expressions for premise and hypothesis",
            premise=premise,
            hypothesis=hypothesis,
            metta_premise=last_metta_premise,
            metta_hypothesis=last_metta_hypothesis,
        )

        if (
            validate_expressions_are_contradictory(
                last_metta_premise, last_metta_hypothesis
            )
            and label == RelationKind.CONTRADICTION
            or label != RelationKind.CONTRADICTION
        ):
            return (last_metta_premise, last_metta_hypothesis, True)

        # If validation failed, generate again with additional context
        additional_context = f"The previous result was not a contradiction as expected. Please ensure the generated MeTTa expressions represent a contradiction. Here are the previous results: {premise=} {hypothesis=} metta_premise={last_metta_premise} metta_hypothesis={last_metta_hypothesis}"

    # Return the last generated expressions even if validation failed
    return (last_metta_premise, last_metta_hypothesis, False)


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
            pl.col(str(TrainingData.label)),
        ]
    )

    # Apply generate_and_validate to all rows
    def process_row(row: dict) -> dict:
        premise = row[str(TrainingData.premise)]
        hypothesis = row[str(TrainingData.hypothesis)]
        label = row[str(TrainingData.label)]
        metta_premise, metta_hypothesis, is_valid = generate_and_validate(
            premise,
            hypothesis,
            label,
            annotation_model=pipeline_config.annotation_model,
        )
        return {
            str(Annotations.metta_premise): metta_premise,
            str(Annotations.metta_hypothesis): metta_hypothesis,
            str(Annotations.is_valid): is_valid,
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
            str(Annotations.is_valid): pl.Boolean,
        },
    )

    # Add the generated columns to the annotated_data_points dataset
    annotated_data_points = (
        unannotated_data_points.head(pipeline_config.subset_size).with_columns(
            [
                generated_results[str(Annotations.metta_premise)],
                generated_results[str(Annotations.metta_hypothesis)],
                generated_results[str(Annotations.is_valid)],
                pl.lit(DATA_VERSION).alias(Annotations.version),
            ]
        )
        # We no longer drop nulls because we want to keep invalid annotations
        # But we might want to filter out rows where generation completely failed (if both are null)?
        # For now, keeping everything as requested by "outputs are stored even if they don't produce a validated output"
    )

    versioned_data = Annotations.validate(annotated_data_points)
    new_result = pl.concat([cached_annotations, versioned_data], how="align")

    new_result.write_parquet(ANNOTATIONS_PATH)

    return new_result
