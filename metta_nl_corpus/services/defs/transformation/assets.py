import random
from typing import Tuple

import polars as pl
from dagster import asset
from structlog import getLogger

from metta_nl_corpus.constants import ANNOTATION_GUIDELINE_PATH, ANNOTATIONS_PATH
from metta_nl_corpus.services.defs.ingestion.assets import (
    DATA_VERSION,
    Annotations,
    TrainingData,
)
from ollama import chat


logger = getLogger(__name__)


def ollama_generate_from_template(text: str, additional_context: str | None = None):
    user_content = 'Turn the following statement into MeTTa expressions that represents the same meaning: "{text}". Return only valid MeTTa expressions with no comments.'.format(
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

    content = response.message.content

    logger.info(f"Generated: {content}")

    if not content:
        return None

    trimmed = content.strip().split("(", 1)[1].rsplit(")", 1)[0]
    if trimmed == "":
        return None

    return f"({trimmed})"


def validate_expressions_are_contradictory(premise: str, hypothesis: str) -> bool:
    # TODO(seb): implement real validation logic
    return random.random() > 0.5


def generate_and_validate(premise: str, hypothesis: str, max_attempts: int = 3) -> Tuple[str | None, str | None]:
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
        
        if metta_premise is None or metta_hypothesis is None:
            return (None, None)
        
        if validate_expressions_are_contradictory(metta_premise, metta_hypothesis):
            return (metta_premise, metta_hypothesis)
    
        # If validation failed, generate again with additional context
        additional_context = f"The previous result was not a contradiction as expected. Please ensure the generated MeTTa expressions represent a contradiction. Here are the previous results: {premise=} {hypothesis=} {metta_premise=} {metta_hypothesis=}"
    
    return (None, None)


@asset()
def data_annotations(
    preprocessed_training_data: pl.DataFrame, cached_annotations: pl.DataFrame
) -> pl.DataFrame:
    logger.info(preprocessed_training_data)
    logger.info(cached_annotations)

    # Get indices not in preprocessed training data
    unannotated_data_points = preprocessed_training_data.filter(
        ~pl.col(str(Annotations.index)).is_in(
            cached_annotations[str(Annotations.index)]
        )
    )

    # Extract premise and hypothesis columns into a simple dataset
    premise_hypothesis_dataset = unannotated_data_points.select([
        pl.col(str(TrainingData.premise)),
        pl.col(str(TrainingData.hypothesis)),
    ])

    # TODO(seb): implement batching
    # Apply generate_and_validate to all rows
    def process_row(row: dict) -> dict:
        premise = row[str(TrainingData.premise)]
        hypothesis = row[str(TrainingData.hypothesis)]
        metta_premise, metta_hypothesis = generate_and_validate(premise, hypothesis)
        return {
            str(Annotations.metta_premise): metta_premise,
            str(Annotations.metta_hypothesis): metta_hypothesis,
        }
    
    # Convert to dict, process rows, and convert back to DataFrame
    rows = premise_hypothesis_dataset.to_dicts()
    processed_rows = [process_row(row) for row in rows]
    generated_results = pl.DataFrame(processed_rows)
    
    # Add the generated columns to the annotated_data_points dataset
    annotated_data_points = unannotated_data_points.with_columns([
        generated_results[str(Annotations.metta_premise)],
        generated_results[str(Annotations.metta_hypothesis)],
        pl.lit(DATA_VERSION).alias(Annotations.version),
    ]).drop_nulls()

    versioned_data = Annotations.validate(annotated_data_points)
    new_result = pl.concat([cached_annotations, versioned_data], how="align")

    new_result.write_parquet(ANNOTATIONS_PATH)

    return new_result
