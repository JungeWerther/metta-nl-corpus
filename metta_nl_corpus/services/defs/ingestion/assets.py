from enum import StrEnum
from pathlib import Path

import polars as pl
from dagster import AssetExecutionContext, Config, asset
from dagster._core.definitions.assets.definition.assets_definition import (
    AssetsDefinition,
)
from huggingface_hub import hf_hub_download
from structlog import getLogger

from metta_nl_corpus.constants import ANNOTATIONS_PATH, VALIDATIONS_PATH
from metta_nl_corpus.lib.caching import create_empty_parquet_from_schema_if_not_exists
from metta_nl_corpus.lib.helpers import Always, Box, info, str_index, with_context
from metta_nl_corpus.lib.interfaces import Fn
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


class Dataset(StrEnum):
    training_data = training_dataset_path
    annotations = str(ANNOTATIONS_PATH)
    validations = str(VALIDATIONS_PATH)


DATASET_SCHEMAS = {
    Dataset.training_data: TrainingData,
    Dataset.annotations: Annotation,
    Dataset.validations: Validation,
}


type Loader = Fn[Path, Box[pl.DataFrame]]


def to_boxed_path_loader(method: Fn[Path, pl.DataFrame]) -> Loader:
    def inner(file_path: Path) -> Box[pl.DataFrame]:
        if not file_path.exists():
            raise FileNotFoundError(f"File not found at {file_path}")

        return Box(data=method(file_path))

    return inner


load_ndjson_from_path = to_boxed_path_loader(pl.read_ndjson)
load_parquet_from_path = to_boxed_path_loader(pl.read_parquet)


def load_annotations(file_path: Path) -> pl.DataFrame:
    df = pl.read_parquet(file_path)
    if "is_valid" not in df.columns:
        df = df.with_columns(pl.lit(True).alias("is_valid"))
    return df


load_annotations_from_path = to_boxed_path_loader(load_annotations)


def make_asset(name: str, _loader: Loader, dataset: Dataset) -> AssetsDefinition:
    def inner(context: AssetExecutionContext, config: BaseConfig) -> pl.DataFrame:
        file_path = Path(dataset)
        data_model = DATASET_SCHEMAS.get(dataset, Always)

        file_path = create_empty_parquet_from_schema_if_not_exists(
            data_model, file_path
        )

        df = (
            _loader(file_path)
            | with_context(context)
            | info(f"Loading {context.asset_key} from {file_path}")
        ).data

        return data_model.validate(df)

    return asset(name=name)(inner)


raw_training_data = make_asset(
    "raw_training_data", load_parquet_from_path, Dataset.training_data
)

cached_annotations = make_asset(
    "cached_annotations", load_annotations_from_path, Dataset.annotations
)

cached_validations = make_asset(
    "cached_validations", load_parquet_from_path, Dataset.validations
)


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
