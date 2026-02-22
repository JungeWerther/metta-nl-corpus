"""Dagster assets for cleaning and re-validating the bronze dataset."""

import multiprocessing
import re
from pathlib import Path

import polars as pl
from dagster import AssetExecutionContext, Config, asset
from huggingface_hub import hf_hub_download
from structlog import getLogger

from metta_nl_corpus.constants import CLEANED_ANNOTATIONS_PATH
from metta_nl_corpus.models import RelationKind
from metta_nl_corpus.services.defs.transformation.assets import (
    get_grounding_space_versions,
    validate_expressions_by_label,
)

logger = getLogger(__name__)

VALIDATION_TIMEOUT_SECONDS = 5

# Patterns that indicate the LLM included natural language instead of MeTTa
_BAD_SYNTAX_PATTERNS = re.compile(
    r"Here is|Rationale|previously|The hypothesis|The premise|"
    r"contradiction|entailment|Note:|Let me|I'll|we need|we can|"
    r"```|Direct contradiction",
    re.IGNORECASE,
)


def migrate_not_to_is_not(expression: str) -> str:
    """Replace (not ...) with (is-not ...) to match updated contradiction syntax."""
    return expression.replace("(not ", "(is-not ")


def has_bad_syntax(expression: str) -> bool:
    """Check if an expression contains natural language instead of valid MeTTa."""
    return bool(_BAD_SYNTAX_PATTERNS.search(expression))


def _run_validation(
    label: str, metta_premise: str, metta_hypothesis: str, queue: multiprocessing.Queue
) -> None:
    """Worker function that runs in a subprocess."""
    try:
        result = validate_expressions_by_label(
            RelationKind(label), metta_premise, metta_hypothesis
        )
        queue.put(result)
    except Exception:
        queue.put(False)


def _validate_with_timeout(
    label: RelationKind, metta_premise: str, metta_hypothesis: str
) -> bool:
    """Run validate_expressions_by_label with a per-row timeout using a subprocess."""
    queue: multiprocessing.Queue = multiprocessing.Queue()
    proc = multiprocessing.Process(
        target=_run_validation,
        args=(label.value, metta_premise, metta_hypothesis, queue),
    )
    proc.start()
    proc.join(timeout=VALIDATION_TIMEOUT_SECONDS)

    if proc.is_alive():
        logger.warning(
            "Validation timed out, killing subprocess",
            timeout_seconds=VALIDATION_TIMEOUT_SECONDS,
            metta_premise=metta_premise[:100],
            metta_hypothesis=metta_hypothesis[:100],
        )
        proc.kill()
        proc.join()
        return False

    if not queue.empty():
        return queue.get_nowait()
    return False


class CleanConfig(Config):
    hf_id: str = "JungeWerther/metta-nl-corpus-bronze-0.1"
    filename: str = "annotations.parquet"


def _log_stats(df: pl.DataFrame, phase: str) -> dict[str, int]:
    """Log dataset statistics and return them as a dict."""
    total = len(df)
    valid_count = df.filter(pl.col("is_valid") == True).height  # noqa: E712
    invalid_count = df.filter(pl.col("is_valid") == False).height  # noqa: E712
    null_premise = df.filter(pl.col("metta_premise").is_null()).height
    null_hypothesis = df.filter(pl.col("metta_hypothesis").is_null()).height

    label_col = "label"
    label_dist = (
        df.group_by(label_col).len().sort(label_col).to_dicts()
        if label_col in df.columns
        else []
    )

    stats = {
        "total": total,
        "valid": valid_count,
        "invalid": invalid_count,
        "null_premise": null_premise,
        "null_hypothesis": null_hypothesis,
    }

    logger.info(
        f"{phase} stats",
        **stats,
        label_distribution=label_dist,
    )

    return stats


@asset
def bronze_dataset(context: AssetExecutionContext, config: CleanConfig) -> pl.DataFrame:
    """Download the bronze dataset from HuggingFace."""
    path = hf_hub_download(
        repo_id=config.hf_id,
        filename=config.filename,
        repo_type="dataset",
    )

    df = pl.read_parquet(Path(path))

    if "is_valid" not in df.columns:
        df = df.with_columns(pl.lit(False).alias("is_valid"))

    logger.info(
        "Loaded bronze dataset",
        rows=len(df),
        columns=df.columns,
        source=config.hf_id,
    )

    return df


@asset
def cleaned_annotations(
    context: AssetExecutionContext,
    config: CleanConfig,
    bronze_dataset: pl.DataFrame,
) -> pl.DataFrame:
    """Re-validate and clean the bronze dataset.

    Only removes rows with bad syntax (natural language in MeTTa fields).
    All other rows are kept with is_valid reflecting re-validation results.
    """
    before_stats = _log_stats(bronze_dataset, "Before cleaning")

    # Drop rows with null metta expressions
    df = bronze_dataset.filter(
        pl.col("metta_premise").is_not_null() & pl.col("metta_hypothesis").is_not_null()
    )

    dropped_nulls = before_stats["total"] - len(df)
    if dropped_nulls > 0:
        logger.info("Dropped rows with null MeTTa expressions", count=dropped_nulls)

    # Filter out rows where the LLM included natural language instead of MeTTa
    bad_syntax_mask = df.select(
        pl.col("metta_premise").map_elements(has_bad_syntax, return_dtype=pl.Boolean)
        | pl.col("metta_hypothesis").map_elements(
            has_bad_syntax, return_dtype=pl.Boolean
        )
    ).to_series()

    bad_syntax_count = bad_syntax_mask.sum()
    df = df.filter(~bad_syntax_mask)
    logger.info(
        "Removed rows with bad syntax (natural language)", count=bad_syntax_count
    )

    # Migrate (not ...) → (is-not ...) syntax
    df = df.with_columns(
        pl.col("metta_premise").map_elements(
            migrate_not_to_is_not, return_dtype=pl.String
        ),
        pl.col("metta_hypothesis").map_elements(
            migrate_not_to_is_not, return_dtype=pl.String
        ),
    )
    logger.info("Migrated (not ...) to (is-not ...) syntax")

    # Add space version hashes
    (
        contradictions_code_hash,
        contradictions_git_hash,
        entailments_code_hash,
        entailments_git_hash,
    ) = get_grounding_space_versions()

    # Re-validate each row
    total_rows = len(df)
    validation_results: list[bool] = []
    for i, row in enumerate(df.iter_rows(named=True)):
        if (i + 1) % 50 == 0 or i == 0:
            logger.info("Validation progress", current=i + 1, total=total_rows)

        label_str = row["label"]
        try:
            label = RelationKind(label_str)
        except ValueError:
            label = RelationKind.NEUTRAL

        is_valid = _validate_with_timeout(
            label=label,
            metta_premise=row["metta_premise"],
            metta_hypothesis=row["metta_hypothesis"],
        )
        validation_results.append(is_valid)

    logger.info("Validation complete", total=total_rows)

    df = df.with_columns(
        pl.Series("is_valid", validation_results),
        pl.lit("0.0.2").alias("version"),
        pl.lit(entailments_code_hash).alias("entailment_space_hash"),
        pl.lit(entailments_git_hash).alias("entailment_git_commit_hash"),
        pl.lit(contradictions_code_hash).alias("contradiction_space_hash"),
        pl.lit(contradictions_git_hash).alias("contradiction_git_commit_hash"),
    )

    after_stats = _log_stats(df, "After cleaning")

    # Delta summary
    logger.info(
        "Cleaning summary",
        rows_before=before_stats["total"],
        rows_after=after_stats["total"],
        rows_removed=before_stats["total"] - after_stats["total"],
        valid_before=before_stats["valid"],
        valid_after=after_stats["valid"],
    )

    # Write output
    CLEANED_ANNOTATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(CLEANED_ANNOTATIONS_PATH)

    logger.info("Wrote cleaned annotations", path=str(CLEANED_ANNOTATIONS_PATH))

    return df
