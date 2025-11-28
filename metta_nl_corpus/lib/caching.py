import os
from pathlib import Path
import polars as pl
from pandera.polars import DataFrameModel

from metta_nl_corpus.lib.helpers import Always


def create_empty_parquet_from_schema_if_not_exists(
    dataframe_model: type[DataFrameModel] | type[Always], file_path: Path
):
    assert isinstance(dataframe_model, DataFrameModel.__class__), (
        "In order to enable caching, you must provide a valid DataFrameModel."
    )

    if not os.path.exists(file_path):
        # Create parent directory if it doesn't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        dtypes = {k: v.type for k, v in dataframe_model.to_schema().dtypes.items()}

        df = pl.DataFrame(schema=dtypes)
        df.write_parquet(file_path)
    return file_path
