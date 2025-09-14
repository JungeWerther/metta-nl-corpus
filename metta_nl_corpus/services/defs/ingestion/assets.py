from enum import StrEnum
from pathlib import Path

from pandera.polars import DataFrameModel

from dagster._core.definitions.assets.definition.assets_definition import (
    AssetsDefinition,
)
import polars as pl
from dagster import AssetExecutionContext, Config, asset

from metta_nl_corpus.lib.helpers import Always, Box, info, with_context
from metta_nl_corpus.lib.interfaces import Fn
from structlog import getLogger

logger = getLogger(__name__)


class TrainingData(DataFrameModel):
    premise: str
    hypothesis: str
    label: int


class BaseConfig(Config):
    dataset_path: str = "datasets"


class Dataset(StrEnum):
    training = "train-00000-of-00001.parquet"


DATASET_SCHEMAS = {Dataset.training: TrainingData}


def get_dataset_path(config: BaseConfig) -> Path:
    """Get the full path to the dataset directory."""
    # Look for datasets relative to the project root
    root_path = Path(__file__).parent.parent.parent.parent.parent
    return root_path / config.dataset_path


type Loader = Fn[Path, Box[pl.DataFrame]]


def to_boxed_path_loader(method: Fn[Path, pl.DataFrame]) -> Loader:
    def inner(file_path: Path) -> Box[pl.DataFrame]:
        if not file_path.exists():
            raise FileNotFoundError(f"File not found at {file_path}")

        return Box(data=method(file_path))

    return inner


load_ndjson_from_path = to_boxed_path_loader(pl.read_ndjson)
load_parquet_from_path = to_boxed_path_loader(pl.read_parquet)


def make_asset(name: str, _loader: Loader, dataset: Dataset) -> AssetsDefinition:
    def inner(context: AssetExecutionContext, config: BaseConfig) -> pl.DataFrame:
        dataset_path = get_dataset_path(config)
        file_path = dataset_path / dataset

        df = (
            _loader(file_path)
            | with_context(context)
            | info(f"Loading {context.asset_key} from {file_path}")
        ).data

        return DATASET_SCHEMAS.get(dataset, Always).validate(df)

    return asset(name=name)(inner)


raw_training_data = make_asset(
    "raw_training_data", load_parquet_from_path, Dataset.training
)


@asset
def preprocessed_training_data(
    context: AssetExecutionContext,
    raw_training_data: pl.DataFrame,
) -> pl.DataFrame:
    """
    Join training data with premises and hypotheses.
    Creates a complete dataset for training with all text fields.
    """

    logger.info(raw_training_data)
    return raw_training_data
