from enum import StrEnum
from pathlib import Path

from pandera.polars import DataFrameModel
from huggingface_hub import hf_hub_download

from dagster._core.definitions.assets.definition.assets_definition import (
    AssetsDefinition,
)
import polars as pl
from dagster import AssetExecutionContext, Config, asset

from metta_nl_corpus.lib.helpers import Always, Box, info, str_index, with_context
from metta_nl_corpus.lib.interfaces import Fn
from structlog import getLogger

logger = getLogger(__name__)

SUBSET_SIZE = 100


class TrainingData(DataFrameModel):
    premise: str
    hypothesis: str
    label: int


training_dataset_path = hf_hub_download(
    repo_id="stanfordnlp/snli",
    filename="plain_text/train-00000-of-00001.parquet",
    repo_type="dataset",
)


class BaseConfig(Config):
    training_dataset: str = training_dataset_path


class Dataset(StrEnum):
    training = training_dataset_path


class RelationKind(StrEnum):
    ENTAILMENT = "entailment"
    NEUTRAL = "neutral"
    CONTRADICTION = "contradication"
    NO_LABEL = "no_label"


DATASET_SCHEMAS = {Dataset.training: TrainingData}


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
        file_path = Path(Dataset.training)

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

    logger.info(
        "unique labels: "
        + str(raw_training_data[str(TrainingData.label)].unique().sort())
    )

    raw_training_data = raw_training_data.head(SUBSET_SIZE).with_columns(
        pl.col(str(TrainingData.label)).map_elements(
            str_index(RelationKind, RelationKind.NO_LABEL), return_dtype=pl.String
        )
    )

    return raw_training_data
