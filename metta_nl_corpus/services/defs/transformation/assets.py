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


def ollama_generate_from_template(text: str):
    response = chat(
        model="gemma3:1b",
        messages=[
            {"role": "system", "content": ANNOTATION_GUIDELINE_PATH.read_text()},
            {
                "role": "user",
                "content": 'Annotate the following statement: "{text}". Return only the MeTTa expression enclosed in parentheses'.format(
                    text=text
                ),
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

    # TODO(seb): implement batching
    annotated_data_points = unannotated_data_points.with_columns(
        pl.col(str(TrainingData.premise))
        .map_elements(ollama_generate_from_template, return_dtype=pl.String)
        .alias(str(Annotations.metta_premise)),
        pl.col(TrainingData.hypothesis)
        .map_elements(ollama_generate_from_template, return_dtype=pl.String)
        .alias(str(Annotations.metta_hypothesis)),
        pl.lit(DATA_VERSION).alias(Annotations.version),
    ).drop_nulls()

    versioned_data = Annotations.validate(annotated_data_points)
    new_result = pl.concat([cached_annotations, versioned_data], how="align")

    new_result.write_parquet(ANNOTATIONS_PATH)

    return new_result
