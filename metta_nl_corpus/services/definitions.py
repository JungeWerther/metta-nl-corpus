from pathlib import Path

from dagster import (
    Definitions,
    FilesystemIOManager,
    ResourceDefinition,
    load_assets_from_modules,
)

from metta_nl_corpus.services.defs.ingestion import assets as ingestion_assets
from metta_nl_corpus.services.defs.transformation import assets as transformation_assets


# Load all assets from the modules
all_assets = [
    *load_assets_from_modules([ingestion_assets]),
    *load_assets_from_modules([transformation_assets]),
]

# Define resources
resources = {
    "io_manager": FilesystemIOManager(
        base_dir=str(Path(__file__).parent.parent / "data")
    ),
    "database_url": ResourceDefinition.string_resource("sqlite:///data.db"),
}

# Create the Dagster definitions
defs = Definitions(
    assets=all_assets,
    resources=resources,
)
