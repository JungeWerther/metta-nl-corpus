import polars as pl
from dagster import asset
from structlog import getLogger


logger = getLogger(__name__)


@asset()
def data_annotations(
    preprocessed_training_data: pl.DataFrame, cached_annotations: pl.DataFrame
) -> None:
    logger.info(preprocessed_training_data)
    logger.info(cached_annotations)
