from pathlib import Path

import polars as pl
from dagster import AssetExecutionContext, Config, asset
from huggingface_hub import hf_hub_download
from structlog import getLogger

from metta_nl_corpus.constants import ANNOTATIONS_DB_PATH
from metta_nl_corpus.lib.helpers import Box, info, str_index, with_context
from metta_nl_corpus.lib.interfaces import Fn
from metta_nl_corpus.lib.storage import AnnotationStore
from metta_nl_corpus.models import (
    Annotation,
    RelationKind,
    TrainingData,
    Validation,
)

logger = getLogger(__name__)

SUBSET_SIZE = 50


training_dataset_path = hf_hub_download(
    repo_id="stanfordnlp/snli",
    filename="plain_text/train-00000-of-00001.parquet",
    repo_type="dataset",
)


class BaseConfig(Config):
    training_dataset: str = training_dataset_path
    version: str | None = None  # <None> for all versions


type Loader = Fn[Path, Box[pl.DataFrame]]


def to_boxed_path_loader(method: Fn[Path, pl.DataFrame]) -> Loader:
    def inner(file_path: Path) -> Box[pl.DataFrame]:
        if not file_path.exists():
            raise FileNotFoundError(f"File not found at {file_path}")

        return Box(data=method(file_path))

    return inner


load_parquet_from_path = to_boxed_path_loader(pl.read_parquet)


@asset
def raw_training_data(
    context: AssetExecutionContext, config: BaseConfig
) -> pl.DataFrame:
    file_path = Path(training_dataset_path)
    df = (
        load_parquet_from_path(file_path)
        | with_context(context)
        | info(f"Loading {context.asset_key} from {file_path}")
    ).data
    return TrainingData.validate(df)


@asset
def cached_annotations(context: AssetExecutionContext) -> pl.DataFrame:
    """Load cached annotations from SQLite store."""
    store = AnnotationStore(ANNOTATIONS_DB_PATH)
    df = store.to_polars("annotations")
    logger.info("Loaded cached annotations from SQLite", count=len(df))
    return Annotation.validate(df)


@asset
def cached_validations(context: AssetExecutionContext) -> pl.DataFrame:
    """Load cached validations from SQLite store."""
    store = AnnotationStore(ANNOTATIONS_DB_PATH)
    df = store.to_polars("validations")
    logger.info("Loaded cached validations from SQLite", count=len(df))
    return Validation.validate(df)


@asset
def preprocessed_training_data(
    raw_training_data: pl.DataFrame,
) -> pl.DataFrame:
    """
    Join training data with premises and hypotheses.
    Creates a complete dataset for training with all text fields.
    """

    df = raw_training_data.with_row_index().with_columns(
        pl.col(str(TrainingData.label)).map_elements(
            str_index(RelationKind, RelationKind.NO_LABEL), return_dtype=pl.String
        )
    )

    return df
