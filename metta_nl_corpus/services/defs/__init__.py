"""Dagster definitions for the MeTTa NL Corpus pipeline."""

from dagster import Definitions, load_assets_from_modules

from . import ingestion, transformation

all_assets = [
    *load_assets_from_modules([ingestion.assets]),
    *load_assets_from_modules([transformation.assets]),
]

defs = Definitions(
    assets=all_assets,
)
