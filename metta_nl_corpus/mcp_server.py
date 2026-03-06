"""MCP server for MeTTa-NL-Corpus pipeline.

Exposes pipeline tools (parsing, validation, generation, querying) and
resources (annotation guideline, MeTTa grounding spaces) via the Model
Context Protocol.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import polars as pl
from mcp.server.fastmcp import FastMCP
from structlog import get_logger

from metta_nl_corpus.constants import (
    ANNOTATION_GUIDELINE_PATH,
    ANNOTATIONS_DB_PATH,
    ANNOTATIONS_PATH,
    PROJECT_ROOT,
    VALIDATIONS_PATH,
)
from metta_nl_corpus.lib.helpers import parse_all
from metta_nl_corpus.lib.runner import create_runner
from metta_nl_corpus.lib.storage import AnnotationStore
from metta_nl_corpus.models import RelationKind

logger = get_logger(__name__)

ENTAILMENTS_PATH = PROJECT_ROOT / "metta_nl_corpus/services/spaces/inference.metta"
CONTRADICTIONS_PATH = (
    PROJECT_ROOT / "metta_nl_corpus/services/spaces/contradictions.metta"
)

mcp = FastMCP(
    "metta-nl-corpus",
    instructions=(
        "MeTTa-NL-Corpus pipeline server. "
        "Provides tools for parsing MeTTa expressions, validating logical relations, "
        "generating NL-to-MeTTa annotations via an internal AI agent, "
        "running batch pipelines, querying stored results, and extracting expressions "
        "from natural language sentences via add_expressions."
    ),
)


# ---------------------------------------------------------------------------
# SQLite-backed annotation store (replaces parquet read-modify-write)
# ---------------------------------------------------------------------------

store = AnnotationStore(ANNOTATIONS_DB_PATH)


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------


@mcp.resource("guideline://annotation")
def annotation_guideline() -> str:
    """The annotation guideline used as the LLM system prompt for MeTTa generation."""
    return ANNOTATION_GUIDELINE_PATH.read_text()


@mcp.resource("space://inference")
def inference_space() -> str:
    """MeTTa grounding space for entailment (transitive inference)."""
    return ENTAILMENTS_PATH.read_text()


@mcp.resource("space://contradictions")
def contradictions_space() -> str:
    """MeTTa grounding space for contradiction detection."""
    return CONTRADICTIONS_PATH.read_text()


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool()
def get_annotation_guideline() -> dict[str, Any]:
    """Read the annotation guideline used as the LLM system prompt.

    Returns the guideline content and its file path for editing.
    """
    return {
        "path": str(ANNOTATION_GUIDELINE_PATH),
        "content": ANNOTATION_GUIDELINE_PATH.read_text(),
    }


@mcp.tool()
def update_annotation_guideline(content: str) -> dict[str, Any]:
    """Replace the annotation guideline with new content.

    Args:
        content: The new guideline content (full replacement).

    Returns confirmation with the file path.
    """
    try:
        ANNOTATION_GUIDELINE_PATH.write_text(content)
        return {"success": True, "path": str(ANNOTATION_GUIDELINE_PATH)}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def add_expressions(
    sentence: str,
    metta_expressions: str,
    model: str = "claude-opus-4-6",
) -> dict[str, Any]:
    """Append validated MeTTa expressions to the annotations cache.

    Parses the expressions, creates an annotation row with label="expression",
    and appends it to the annotations parquet file.

    Args:
        sentence: The original natural-language sentence.
        metta_expressions: The generated MeTTa expressions (newline-separated).
        model: Which model generated them (default: "claude").

    Returns the annotation_id on success, or an error.
    """
    try:
        atoms = parse_all(metta_expressions)
    except Exception as e:
        return {"success": False, "error": f"MeTTa parse error: {e}"}

    if not atoms:
        return {"success": False, "error": "No valid MeTTa atoms found."}

    annotation_id = str(uuid.uuid4())
    system_prompt = ANNOTATION_GUIDELINE_PATH.read_text()

    try:
        store.insert_annotation(
            {
                "annotation_id": annotation_id,
                "index": 0,
                "premise": sentence,
                "hypothesis": None,
                "label": RelationKind.EXPRESSION.value,
                "metta_premise": metta_expressions.strip(),
                "metta_hypothesis": None,
                "generation_model": model,
                "system_prompt": system_prompt,
                "version": "0.0.4",
                "is_valid": True,
                "input_tokens": None,
                "output_tokens": None,
            }
        )
        logger.info(
            "Stored expression annotation",
            annotation_id=annotation_id,
        )
    except Exception as e:
        return {"success": False, "error": f"Failed to store annotation: {e}"}

    return {
        "success": True,
        "annotation_id": annotation_id,
        "atoms_count": len(atoms),
        "atoms": [str(a) for a in atoms],
    }


@mcp.tool()
def parse_metta(metta_code: str) -> dict[str, Any]:
    """Parse and validate MeTTa syntax.

    Returns a list of parsed atoms on success, or an error message on failure.
    """
    try:
        atoms = parse_all(metta_code)
        return {
            "success": True,
            "atoms": [str(a) for a in atoms],
            "error": None,
        }
    except Exception as e:
        return {"success": False, "atoms": [], "error": str(e)}


@mcp.tool()
def execute_metta(
    metta_code: str,
    premise: str | None = None,
    store_result: bool = False,
) -> dict[str, Any]:
    """Execute MeTTa code in a fresh runner and return the results.

    Runs arbitrary MeTTa programs (reductions, proofs, queries) through
    the full MeTTa runtime. Each call gets an isolated runner.

    Args:
        metta_code: MeTTa source code to execute.
        premise: Optional natural-language premise for the annotation.
        store_result: Whether to store the result as an annotation (default: False).
            Requires premise to be set.

    Returns dict with ``results`` (list of result strings) or ``error``.
    """
    try:
        runner = create_runner()
        raw_results = runner.run(metta_code)
        # raw_results is a list of lists (one per !-expression)
        results = []
        for group in raw_results:
            results.append([str(atom) for atom in group])
    except Exception as e:
        return {"success": False, "error": str(e)}

    response: dict[str, Any] = {"success": True, "results": results}

    if store_result and premise is not None:
        annotation_id = str(uuid.uuid4())
        system_prompt = ANNOTATION_GUIDELINE_PATH.read_text()

        try:
            store.insert_annotation(
                {
                    "annotation_id": annotation_id,
                    "index": 0,
                    "premise": premise,
                    "hypothesis": None,
                    "label": RelationKind.EXPRESSION.value,
                    "metta_premise": metta_code.strip(),
                    "metta_hypothesis": None,
                    "generation_model": "claude-opus-4-6",
                    "system_prompt": system_prompt,
                    "version": "0.0.4",
                    "is_valid": True,
                    "input_tokens": None,
                    "output_tokens": None,
                }
            )
            logger.info(
                "Stored execute_metta annotation",
                annotation_id=annotation_id,
                premise=premise,
            )
            response["annotation_id"] = annotation_id
        except Exception as e:
            logger.error("Failed to store annotation", error=str(e))
            response["store_error"] = str(e)

    return response


@mcp.tool()
def validate_relation(
    metta_premise: str,
    metta_hypothesis: str,
    relation: str,
    premise: str | None = None,
    hypothesis: str | None = None,
    model: str = "claude-opus-4-6",
    store_result: bool = False,
) -> dict[str, Any]:
    """Check whether MeTTa expressions satisfy a logical relation.

    By default, only validates without storing. Set store_result=True to
    persist the result as a new annotation.

    Args:
        metta_premise: MeTTa s-expression(s) for the premise.
        metta_hypothesis: MeTTa s-expression(s) for the hypothesis.
        relation: Expected relation — one of "entailment", "contradiction", "neutral".
        premise: Original natural-language premise (stored with the annotation).
        hypothesis: Original natural-language hypothesis (stored with the annotation).
        model: Which model generated the expressions (default: "claude").
        store_result: Whether to store the result as a new annotation (default: False).

    Returns dict with ``valid`` (bool), ``message``, and optionally ``annotation_id``.
    """
    from metta_nl_corpus.services.defs.transformation.assets import (
        validate_expressions_by_label,
    )

    try:
        label = RelationKind(relation.lower().strip())
    except ValueError:
        return {
            "valid": False,
            "message": f"Unknown relation '{relation}'. Use entailment, neutral, or contradiction.",
        }

    try:
        is_valid = validate_expressions_by_label(
            label=label,
            metta_premise=metta_premise.strip(),
            metta_hypothesis=metta_hypothesis.strip(),
        )
    except Exception as e:
        return {"valid": False, "message": f"Validation failed: {e}"}

    result: dict[str, Any] = {
        "valid": is_valid,
        "message": f"Expressions {'match' if is_valid else 'do NOT match'} expected relation {label.value}.",
    }

    if store_result:
        annotation_id = str(uuid.uuid4())
        system_prompt = ANNOTATION_GUIDELINE_PATH.read_text()

        try:
            store.insert_annotation(
                {
                    "annotation_id": annotation_id,
                    "index": 0,
                    "premise": premise,
                    "hypothesis": hypothesis,
                    "label": label.value,
                    "metta_premise": metta_premise.strip(),
                    "metta_hypothesis": metta_hypothesis.strip(),
                    "generation_model": model,
                    "system_prompt": system_prompt,
                    "version": "0.0.4",
                    "is_valid": is_valid,
                    "input_tokens": None,
                    "output_tokens": None,
                }
            )
            logger.info(
                "Stored validated annotation",
                annotation_id=annotation_id,
                is_valid=is_valid,
            )
            result["annotation_id"] = annotation_id
        except Exception as e:
            logger.error("Failed to store annotation", error=str(e))
            result["message"] += f" WARNING: failed to store: {e}"

    return result


@mcp.tool()
async def ask_metta_agent(
    premise: str,
    hypothesis: str,
    relation: str,
    model: str = "openai:gpt-4o-mini",
) -> dict[str, Any]:
    """Use the pipeline's internal Pydantic AI agent to generate MeTTa expressions.

    The agent has the annotation guideline as its system prompt and uses
    parse_all and validate_relation as internal tools to self-correct.

    Args:
        premise: Natural-language premise sentence.
        hypothesis: Natural-language hypothesis sentence.
        relation: Expected relation — "entailment", "contradiction", or "neutral".
        model: Model identifier (e.g. "openai:gpt-4o-mini", "openai:gpt-4o").

    Returns the generated MeTTa expressions, validation status, and token usage.
    """
    from metta_nl_corpus.services.defs.transformation.assets import (
        ExpressionDeps,
        _create_metta_agent,
        last_generation_attempt,
        validate_expressions_by_label,
    )

    try:
        label = RelationKind(relation.lower().strip())
    except ValueError:
        return {
            "error": f"Unknown relation '{relation}'. Use entailment, neutral, or contradiction.",
        }

    system_prompt = ANNOTATION_GUIDELINE_PATH.read_text()
    agent = _create_metta_agent(system_prompt, model)
    deps = ExpressionDeps(premise=premise, hypothesis=hypothesis, label=label)

    # Reset so we don't leak a previous call's attempt
    last_generation_attempt.set(None)

    try:
        result = await agent.run(
            "Generate MeTTa expressions for the premise and hypothesis.",
            deps=deps,
        )
    except Exception as e:
        last_attempt = last_generation_attempt.get()
        return {
            "error": f"Agent execution failed: {e}",
            "last_attempt": last_attempt,
        }

    output = result.output
    usage = result.usage()

    is_valid = validate_expressions_by_label(
        label, output.metta_premise, output.metta_hypothesis
    )

    input_tokens = getattr(usage, "input_tokens", None)
    output_tokens = getattr(usage, "output_tokens", None)

    # Store annotation
    try:
        store.insert_annotation(
            {
                "annotation_id": str(uuid.uuid4()),
                "index": 0,
                "premise": premise,
                "hypothesis": hypothesis,
                "label": label.value,
                "metta_premise": output.metta_premise,
                "metta_hypothesis": output.metta_hypothesis,
                "generation_model": model,
                "system_prompt": system_prompt,
                "version": "0.0.4",
                "is_valid": is_valid,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            }
        )
        logger.info("Stored annotation")
    except Exception as e:
        logger.error("Failed to store annotation", error=str(e))

    return {
        "metta_premise": output.metta_premise,
        "metta_hypothesis": output.metta_hypothesis,
        "relation": output.relation,
        "is_valid": is_valid,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }


@mcp.tool()
async def generate_and_validate(
    premise: str,
    hypothesis: str,
    relation: str,
    index: int = 0,
    model: str = "openai:gpt-4o-mini",
) -> dict[str, Any]:
    """Full single-pair generation + validation (one-shot).

    Runs the complete pipeline for a single premise/hypothesis pair:
    creates an agent, generates MeTTa expressions, and validates them.

    Args:
        premise: Natural-language premise sentence.
        hypothesis: Natural-language hypothesis sentence.
        relation: Expected relation — "entailment", "contradiction", or "neutral".
        index: Training data index (default 0).
        model: Model identifier for generation.

    Returns annotation and validation details, or an error.
    """
    from metta_nl_corpus.services.defs.transformation.assets import (
        generate_and_validate_async,
    )

    try:
        label = RelationKind(relation.lower().strip())
    except ValueError:
        return {
            "error": f"Unknown relation '{relation}'. Use entailment, neutral, or contradiction.",
        }

    result = await generate_and_validate_async(
        premise=premise,
        hypothesis=hypothesis,
        label=label,
        index=index,
        annotation_model=model,
    )

    if result.annotation is None:
        return {"error": "Generation failed — no annotation produced."}

    annotation_row = result.annotation.to_dicts()[0]
    validation_row = (
        result.validation.to_dicts()[0] if result.validation is not None else None
    )

    return {
        "annotation": annotation_row,
        "validation": validation_row,
    }


@mcp.tool()
async def run_pipeline(
    subset_size: int = 10,
    batch_size: int = 10,
    annotation_model: str = "openai:gpt-5-nano",
    hf_id: str = "squad",
    filename: str = "train.parquet",
    split: str = "train",
    offset: int = 0,
) -> dict[str, Any]:
    """Execute the full batch annotation pipeline.

    Args:
        subset_size: Number of samples to process.
        batch_size: Batch size for concurrent processing.
        annotation_model: Model to use for annotation generation.
        hf_id: HuggingFace dataset ID.
        filename: Filename within the dataset repository.
        split: Dataset split.
        offset: Start processing from this training data index.

    Returns execution status and result counts.
    """
    from metta_nl_corpus.lib.pipeline_config import DatasetConfig, PipelineRunConfig
    from metta_nl_corpus.services.pipeline_executor import PipelineExecutor

    if subset_size < 2:
        return {"error": "subset_size must be at least 2."}

    dataset_config = DatasetConfig(
        hf_id=hf_id,
        filename=filename,
        split=split,
        repo_type="dataset",
    )

    pipeline_config = PipelineRunConfig(
        dataset_config=dataset_config,
        model_name="mcp-run",
        version="v1",
        subset_size=subset_size,
        batch_size=batch_size,
        annotation_model=annotation_model,
        offset=offset,
    )

    executor = PipelineExecutor()
    result = await executor.execute_pipeline(pipeline_config)

    return {
        "status": result.status,
        "cache_key": result.cache_key,
        "annotations_path": result.annotations_path,
        "annotations_count": result.annotations_count,
        "error": result.error,
    }


@mcp.tool()
def query_annotations(
    file: str = "annotations",
    filter_column: str | None = None,
    filter_value: str | list[str] | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Query annotations or validations parquet files.

    Args:
        file: Which file to query — "annotations", "validations", or "cleaned".
        filter_column: Optional column name to filter on.
        filter_value: Value or list of values to match in the filter column.
        limit: Maximum number of rows to return (default 20).

    Returns matching rows as a list of dicts, plus the total count.
    """
    valid_files = {"annotations", "validations", "cleaned"}
    if file not in valid_files:
        return {"error": f"Unknown file '{file}'. Use: {', '.join(valid_files)}"}

    # 'cleaned' still reads from parquet (separate pipeline output)
    if file == "cleaned":
        cleaned_path = PROJECT_ROOT / "datasets" / "cleaned_annotations.parquet"
        if not cleaned_path.exists():
            return {"error": f"File not found: {cleaned_path}"}
        try:
            df = pl.read_parquet(cleaned_path)
        except Exception as e:
            return {"error": f"Failed to read parquet: {e}"}
        if filter_column and filter_value:
            if filter_column not in df.columns:
                return {
                    "error": f"Column '{filter_column}' not found. Available: {df.columns}"
                }
            col = pl.col(filter_column).cast(pl.Utf8)
            if isinstance(filter_value, list):
                df = df.filter(col.is_in(filter_value))
            else:
                df = df.filter(col == filter_value)
        total = len(df)
        rows = df.head(limit).to_dicts()
        columns = df.columns
    else:
        result = store.query(
            table=file,
            filter_column=filter_column,
            filter_value=filter_value,
            limit=limit,
        )
        if "error" in result:
            return result
        total = result["total"]
        rows = result["rows"]
        columns = result["columns"]

    # Truncate long string values to keep MCP response size manageable
    max_cell_len = 200
    for row in rows:
        for key, val in row.items():
            if isinstance(val, str) and len(val) > max_cell_len:
                row[key] = val[:max_cell_len] + "..."

    return {
        "total": total,
        "returned": len(rows),
        "columns": columns,
        "rows": rows,
    }


@mcp.tool()
def update_annotation(
    annotation_id: str,
    metta_premise: str,
    metta_hypothesis: str,
    fix_reason: str | None = None,
) -> dict[str, Any]:
    """Update a single annotation with human-corrected MeTTa expressions.

    Re-validates the corrected expressions and tags the version as human-annotated.

    Args:
        annotation_id: The UUID of the annotation to update.
        metta_premise: Corrected MeTTa premise expression(s).
        metta_hypothesis: Corrected MeTTa hypothesis expression(s).
        fix_reason: Why the correction was made (e.g. missing is-not negation, entity mismatch).

    Returns the updated row or an error.
    """
    from metta_nl_corpus.services.defs.transformation.assets import (
        validate_expressions_by_label,
    )

    existing = store.get_annotation(annotation_id)
    if existing is None:
        return {"error": f"Annotation '{annotation_id}' not found."}

    label_str = existing.get("label", "")
    # Normalize common typo in source data
    if label_str == "contradication":
        label_str = "contradiction"
    try:
        label = RelationKind(label_str)
    except ValueError:
        return {"error": f"Unknown label '{label_str}' on annotation."}

    is_valid = validate_expressions_by_label(
        label=label,
        metta_premise=metta_premise.strip(),
        metta_hypothesis=metta_hypothesis.strip(),
    )

    store.update_annotation(
        annotation_id,
        {
            "metta_premise": metta_premise.strip(),
            "metta_hypothesis": metta_hypothesis.strip(),
            "is_valid": is_valid,
            "fix_reason": fix_reason,
        },
    )

    logger.info(
        "Updated annotation",
        annotation_id=annotation_id,
        is_valid=is_valid,
        fix_reason=fix_reason,
    )

    updated = store.get_annotation(annotation_id)
    # Truncate system_prompt for response size
    if (
        updated
        and "system_prompt" in updated
        and isinstance(updated["system_prompt"], str)
    ):
        updated["system_prompt"] = updated["system_prompt"][:100] + "..."

    return {"success": True, "is_valid": is_valid, "annotation": updated}


@mcp.tool()
def clean_annotation(
    annotation_id: str,
    metta_premise: str | None = None,
    metta_hypothesis: str | None = None,
    comment: str | None = None,
) -> dict[str, Any]:
    """Fix and re-validate a single annotation (upsert).

    Allows patching either metta_premise, metta_hypothesis, or both.
    Unchanged fields keep their current value. Re-validates after patching
    and records modification_date and modification_comment.

    If the annotation_id does not exist, inserts a new row.

    Args:
        annotation_id: The UUID of the annotation to clean.
        metta_premise: New MeTTa premise (or None to keep existing).
        metta_hypothesis: New MeTTa hypothesis (or None to keep existing).
        comment: Why the change was made.

    Returns the updated row with validation result.
    """
    from datetime import datetime, timezone

    from metta_nl_corpus.services.defs.transformation.assets import (
        validate_expressions_by_label,
    )

    existing = store.get_annotation(annotation_id)

    premise = (
        metta_premise.strip()
        if metta_premise
        else (existing.get("metta_premise") if existing else "")
    ) or ""
    hypothesis = (
        metta_hypothesis.strip()
        if metta_hypothesis
        else (existing.get("metta_hypothesis") if existing else "")
    ) or ""

    if not premise and not hypothesis:
        return {"error": "Both metta_premise and metta_hypothesis are empty."}

    label_str = existing.get("label", "") if existing else ""
    if label_str == "contradication":
        label_str = "contradiction"
    if not label_str:
        return {
            "error": "Cannot determine label — provide an existing annotation_id or use add_expressions."
        }
    try:
        label = RelationKind(label_str)
    except ValueError:
        return {"error": f"Unknown label '{label_str}' on annotation."}

    is_valid = validate_expressions_by_label(
        label=label,
        metta_premise=premise,
        metta_hypothesis=hypothesis,
    )

    now = datetime.now(timezone.utc).isoformat()
    row = {
        "annotation_id": annotation_id,
        "metta_premise": premise,
        "metta_hypothesis": hypothesis,
        "is_valid": is_valid,
        "modification_date": now,
        "modification_comment": comment,
    }
    # Carry forward existing fields for upsert when inserting new
    if existing:
        for key in (
            "index",
            "premise",
            "hypothesis",
            "label",
            "generation_model",
            "system_prompt",
            "version",
        ):
            if key not in row:
                row[key] = existing.get(key)

    store.upsert_annotation(row)

    logger.info(
        "Cleaned annotation",
        annotation_id=annotation_id,
        is_valid=is_valid,
        comment=comment,
    )

    updated = store.get_annotation(annotation_id)
    if updated and isinstance(updated.get("system_prompt"), str):
        updated["system_prompt"] = updated["system_prompt"][:100] + "..."

    return {
        "success": True,
        "is_valid": is_valid,
        "annotation": updated,
    }


@mcp.tool()
def export_annotations_parquet(
    table: str = "annotations",
) -> dict[str, Any]:
    """Export the SQLite annotation store to a parquet file.

    Use this to generate parquet files for HuggingFace dataset uploads.

    Args:
        table: Which table to export — "annotations" or "validations".

    Returns the export path and row count.
    """
    path_map = {
        "annotations": ANNOTATIONS_PATH,
        "validations": VALIDATIONS_PATH,
    }
    path = path_map.get(table)
    if path is None:
        return {"error": f"Unknown table '{table}'. Use: {', '.join(path_map)}"}

    try:
        count = store.export_parquet(path, table=table)
        return {"success": True, "path": str(path), "rows": count}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def import_annotations_parquet(
    path: str | None = None,
) -> dict[str, Any]:
    """Import annotations from a parquet file into the SQLite store.

    Use this to migrate existing parquet data into the new SQLite backend.

    Args:
        path: Path to parquet file. Defaults to the standard annotations.parquet.

    Returns the number of rows imported.
    """
    parquet_path = Path(path) if path else ANNOTATIONS_PATH
    if not parquet_path.exists():
        return {"error": f"File not found: {parquet_path}"}

    try:
        count = store.import_parquet(parquet_path, table="annotations")
        return {"success": True, "rows_imported": count, "source": str(parquet_path)}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def yield_unannotated_pairs(
    limit: int = 50,
    offset: int = 10_000,
    label: str | None = None,
) -> dict[str, Any]:
    """Yield premise/hypothesis pairs from SNLI that are not yet in the annotation store.

    Scans the SNLI training set starting at `offset`, skips pairs whose
    (premise, hypothesis) already exist in SQLite, and returns up to `limit`
    unannotated pairs ready for annotation.

    Args:
        limit: Maximum number of unannotated pairs to return (default 50).
        offset: Starting row index in the SNLI dataset (default 10_000).
        label: Optional label filter — "entailment", "contradiction", or "neutral".

    Returns a list of dicts with premise, hypothesis, label, and snli_index.
    """
    from metta_nl_corpus.lib.helpers import str_index

    snli_path = Path(
        __import__("huggingface_hub").hf_hub_download(
            repo_id="stanfordnlp/snli",
            filename="plain_text/train-00000-of-00001.parquet",
            repo_type="dataset",
        )
    )
    df = pl.read_parquet(snli_path)

    # Map integer labels to strings
    label_fn = str_index(RelationKind, RelationKind.NO_LABEL)
    df = df.with_row_index("snli_index").with_columns(
        pl.col("label").map_elements(label_fn, return_dtype=pl.Utf8).alias("label_str")
    )

    # Slice from offset
    df = df.filter(pl.col("snli_index") >= offset)

    # Filter by label if requested
    if label:
        df = df.filter(pl.col("label_str") == label.lower().strip())

    # Exclude no_label rows
    df = df.filter(pl.col("label_str") != RelationKind.NO_LABEL.value)

    # Get existing (premise, hypothesis) pairs from SQLite
    conn = store._get_conn()
    existing = {
        (row[0], row[1])
        for row in conn.execute(
            "SELECT premise, hypothesis FROM annotations"
        ).fetchall()
    }

    results: list[dict[str, Any]] = []
    for row in df.iter_rows(named=True):
        if len(results) >= limit:
            break
        key = (row["premise"], row["hypothesis"])
        if key not in existing:
            results.append(
                {
                    "snli_index": row["snli_index"],
                    "premise": row["premise"],
                    "hypothesis": row["hypothesis"],
                    "label": row["label_str"],
                }
            )

    return {
        "total_available": len(results),
        "offset": offset,
        "pairs": results,
    }


@mcp.tool()
def revalidate_annotations(
    label: str | None = None,
    save: bool = True,
    timeout: int = 5,
    limit: int = 10,
    offset: int = 0,
) -> dict[str, Any]:
    """Re-validate stored annotations against the current inference engine.

    Runs validate_expressions_by_label on each annotation and returns a
    summary of results. Only persists changes when save=True.

    Args:
        label: Optional label filter (e.g. "entailment", "contradiction", "neutral").
        save: Whether to persist updated is_valid flags to the DB. Default True.
        timeout: Per-row validation timeout in seconds (default 5).
        limit: Max rows to process (0 = all, default 10).
        offset: Number of rows to skip before processing (default 0).

    Returns a summary with per-row results and aggregate counts.
    """
    from metta_nl_corpus.services.defs.cleaning.assets import (
        has_bad_syntax,
        migrate_not_to_is_not,
    )
    from metta_nl_corpus.services.defs.transformation.assets import (
        get_grounding_space_versions,
        validate_expressions_by_label,
    )

    conn = store._get_conn()

    query = "SELECT * FROM annotations"
    params: list[str] = []
    if label:
        query += " WHERE label = ?"
        params.append(label)
    query += " ORDER BY annotation_id"
    if limit > 0:
        query += " LIMIT ?"
        params.append(str(limit))
    if offset > 0:
        query += " OFFSET ?"
        params.append(str(offset))

    rows = [
        store._row_to_dict(dict(r), source="annotations")
        for r in conn.execute(query, params).fetchall()
    ]

    if not rows:
        return {"error": "No annotations found matching filter."}

    (
        contradictions_code_hash,
        contradictions_git_hash,
        entailments_code_hash,
        entailments_git_hash,
    ) = get_grounding_space_versions()

    results: list[dict[str, Any]] = []
    counts = {"total": len(rows), "valid": 0, "invalid": 0, "skipped": 0, "changed": 0}

    for i, row in enumerate(rows):
        aid = row["annotation_id"]
        premise = row.get("metta_premise")
        hypothesis = row.get("metta_hypothesis")
        row_label = row.get("label", "")

        # Skip rows without both expressions
        if not premise or not hypothesis:
            results.append(
                {
                    "annotation_id": aid,
                    "status": "skipped",
                    "reason": "missing_expressions",
                }
            )
            counts["skipped"] += 1
            continue

        # Skip bad syntax
        if has_bad_syntax(premise) or has_bad_syntax(hypothesis):
            results.append(
                {"annotation_id": aid, "status": "skipped", "reason": "bad_syntax"}
            )
            counts["skipped"] += 1
            continue

        # Migrate (not ...) → (is-not ...)
        premise = migrate_not_to_is_not(premise)
        hypothesis = migrate_not_to_is_not(hypothesis)

        try:
            rel = RelationKind(row_label)
        except ValueError:
            rel = RelationKind.NEUTRAL

        # Validate directly in-process (like the tests do)
        try:
            is_valid = validate_expressions_by_label(rel, premise, hypothesis)
            status = "valid" if is_valid else "invalid"
        except Exception as e:
            is_valid = False
            status = "error"
            logger.warning("Validation error", annotation_id=aid, error=str(e))

        old_valid = row.get("is_valid", False)
        changed = old_valid != is_valid

        if is_valid:
            counts["valid"] += 1
        else:
            counts["invalid"] += 1
        if changed:
            counts["changed"] += 1

        entry: dict[str, Any] = {
            "annotation_id": aid,
            "label": row_label,
            "status": status,
            "was_valid": old_valid,
            "is_valid": is_valid,
            "changed": changed,
        }
        results.append(entry)

        if save and (changed or premise != row.get("metta_premise")):
            store.update_annotation(
                aid,
                {
                    "metta_premise": premise,
                    "metta_hypothesis": hypothesis,
                    "is_valid": is_valid,
                },
            )

        if (i + 1) % 50 == 0:
            logger.info("Revalidation progress", current=i + 1, total=len(rows))

    logger.info("Revalidation complete", save=save, **counts)

    return {
        "save": save,
        "space_versions": {
            "entailment_hash": entailments_code_hash,
            "entailment_git": entailments_git_hash,
            "contradiction_hash": contradictions_code_hash,
            "contradiction_git": contradictions_git_hash,
        },
        "counts": counts,
        "results": results[:100],
        "truncated": len(results) > 100,
    }
