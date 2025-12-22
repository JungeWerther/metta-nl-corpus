from enum import StrEnum
from typing import NamedTuple

from pandera.polars import DataFrameModel, Field
from pandera.typing.common import UInt32
from pandera.typing.polars import DataFrame

DATA_VERSION = "0.0.1"


class TrainingBase(DataFrameModel):
    premise: str
    hypothesis: str

    class Config:
        coerce = True


class TrainingData(TrainingBase):
    label: int

    class Config:
        coerce = True


class Annotation(TrainingBase):
    """Generated MeTTa annotations for premise-hypothesis pairs."""

    annotation_id: str  # UUID for this annotation
    index: UInt32  # Reference to training data index
    label: str
    metta_premise: str | None
    metta_hypothesis: str | None
    generation_model: str  # Model used for generation
    system_prompt: str  # System prompt used for generation
    metta_premise_prompt: str  # Prompt used to generate metta_premise
    metta_hypothesis_prompt: str  # Prompt used to generate metta_hypothesis
    version: str = Field(default=DATA_VERSION)

    class Config:
        coerce = True


class Validation(DataFrameModel):
    """Validation results for generated annotations."""

    validation_id: str  # UUID for this validation
    annotation_id: str  # Reference to Annotations.annotation_id
    is_valid: bool  # Whether validation passed
    relation_kind: str  # RelationKind

    entailment_space_hash: str  # MD5 hash of the MeTTa entailment space file
    entailment_git_commit_hash: str | None  # Git commit hash of the space file

    contradiction_space_hash: str  # MD5 hash of the MeTTa contraditiction space file
    contradiction_git_commit_hash: str | None  # Git commit hash of the space file

    validation_timestamp: str  # ISO timestamp

    class Config:
        coerce = True


class RelationKind(StrEnum):
    ENTAILMENT = "entailment"
    NEUTRAL = "neutral"
    CONTRADICTION = "contradication"
    NO_LABEL = "no_label"


class GenerationFailureReason(StrEnum):
    NO_CONTENT = "no_content"
    EMPTY_EXPRESSION = "empty_expression"


class GenerationResult(NamedTuple):
    expression: str | None
    failure_reason: GenerationFailureReason | None


class GenerateAndValidateResult(NamedTuple):
    """Combined result of generation and validation."""

    annotation: DataFrame[Annotation] | None
    validation: DataFrame[Validation] | None
