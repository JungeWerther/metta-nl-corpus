"""Configuration models for dynamic dataset pipeline execution."""

from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


class DatasetConfig(BaseModel):
    """Configuration for a dataset to be processed by the pipeline."""

    hf_id: str = Field(description="HuggingFace dataset ID")
    repo_type: str = Field(default="dataset", description="Repository type")
    filename: str = Field(description="Filename within the dataset repository")
    subset: Optional[str] = Field(default=None, description="Dataset subset name")
    split: Optional[str] = Field(default="train", description="Dataset split")
    cache_dir: Optional[Path] = Field(
        default=None, description="Custom cache directory"
    )

    class Config:
        arbitrary_types_allowed = True


class PipelineRunConfig(BaseModel):
    """Configuration for a complete pipeline run."""

    dataset_config: DatasetConfig
    model_name: str
    version: str
    subset_size: int = 3
    batch_size: int = 10
    annotation_model: str = "gemma3:1b"

    @property
    def cache_key(self) -> str:
        """Generate a unique cache key for this pipeline configuration."""
        return f"{self.dataset_config.hf_id}_{self.model_name}_{self.version}".replace(
            "/", "_"
        )

    @property
    def annotations_path(self) -> Path:
        """Get the path for annotations for this specific pipeline run."""
        from metta_nl_corpus.constants import PROJECT_ROOT

        cache_dir = PROJECT_ROOT / "datasets" / "annotations" / self.cache_key
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / "annotations.parquet"
