"""Shared data source for yielding unannotated SNLI pairs."""

from __future__ import annotations

from pathlib import Path
from typing import NamedTuple

import polars as pl
from structlog import get_logger

from metta_nl_corpus.lib.helpers import str_index
from metta_nl_corpus.lib.storage import AnnotationStore
from metta_nl_corpus.models import RelationKind

logger = get_logger(__name__)


class UnannotatedPair(NamedTuple):
    snli_index: int
    premise: str
    hypothesis: str
    label: str


def _load_snli() -> pl.DataFrame:
    """Download (cached) and load the SNLI training set."""
    snli_path = Path(
        __import__("huggingface_hub").hf_hub_download(
            repo_id="stanfordnlp/snli",
            filename="plain_text/train-00000-of-00001.parquet",
            repo_type="dataset",
        )
    )
    return pl.read_parquet(snli_path)


def yield_unannotated_pairs(
    store: AnnotationStore,
    *,
    limit: int = 50,
    offset: int = 10_000,
    label: str | None = None,
) -> list[UnannotatedPair]:
    """Return unannotated SNLI pairs not yet in the annotation store.

    Scans the SNLI training set starting at ``offset``, skips pairs whose
    (premise, hypothesis) already exist in SQLite, and returns up to
    ``limit`` unannotated pairs.
    """
    df = _load_snli()

    label_fn = str_index(RelationKind, RelationKind.NO_LABEL)
    df = df.with_row_index("snli_index").with_columns(
        pl.col("label").map_elements(label_fn, return_dtype=pl.Utf8).alias("label_str")
    )

    df = df.filter(pl.col("snli_index") >= offset)

    if label:
        df = df.filter(pl.col("label_str") == label.lower().strip())

    df = df.filter(pl.col("label_str") != RelationKind.NO_LABEL.value)

    conn = store._get_conn()
    existing = {
        (row[0], row[1])
        for row in conn.execute(
            "SELECT premise, hypothesis FROM annotations"
        ).fetchall()
    }

    results: list[UnannotatedPair] = []
    for row in df.iter_rows(named=True):
        if len(results) >= limit:
            break
        key = (row["premise"], row["hypothesis"])
        if key not in existing:
            results.append(
                UnannotatedPair(
                    snli_index=row["snli_index"],
                    premise=row["premise"],
                    hypothesis=row["hypothesis"],
                    label=row["label_str"],
                )
            )

    return results
