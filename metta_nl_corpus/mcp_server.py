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
    ANNOTATIONS_PATH,
    PROJECT_ROOT,
    VALIDATIONS_PATH,
)
from metta_nl_corpus.lib.helpers import parse_all
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
        "running batch pipelines, querying stored results, and extracting ontologies "
        "from natural language sentences via add_expressions."
    ),
)


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
    model: str = "claude",
) -> dict[str, Any]:
    """Append validated MeTTa ontology expressions to the annotations cache.

    Parses the expressions, creates an annotation row with label="ontology",
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

    row = pl.DataFrame(
        [
            {
                "annotation_id": annotation_id,
                "index": 0,
                "premise": sentence,
                "hypothesis": None,
                "label": RelationKind.ONTOLOGY.value,
                "metta_premise": metta_expressions.strip(),
                "metta_hypothesis": None,
                "generation_model": model,
                "system_prompt": system_prompt,
                "version": "0.0.3",
                "is_valid": True,
                "input_tokens": None,
                "output_tokens": None,
            }
        ],
        schema={
            "annotation_id": pl.Utf8,
            "index": pl.UInt32,
            "premise": pl.Utf8,
            "hypothesis": pl.Utf8,
            "label": pl.Utf8,
            "metta_premise": pl.Utf8,
            "metta_hypothesis": pl.Utf8,
            "generation_model": pl.Utf8,
            "system_prompt": pl.Utf8,
            "version": pl.Utf8,
            "is_valid": pl.Boolean,
            "input_tokens": pl.Int64,
            "output_tokens": pl.Int64,
        },
    )

    try:
        if ANNOTATIONS_PATH.exists():
            existing = pl.read_parquet(ANNOTATIONS_PATH)
            combined = pl.concat([existing, row], how="diagonal_relaxed")
        else:
            ANNOTATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
            combined = row
        combined.write_parquet(ANNOTATIONS_PATH)
        logger.info(
            "Appended ontology annotation",
            annotation_id=annotation_id,
            path=str(ANNOTATIONS_PATH),
        )
    except Exception as e:
        return {"success": False, "error": f"Failed to write parquet: {e}"}

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
) -> dict[str, Any]:
    """Execute MeTTa code in a fresh runner and return the results.

    Runs arbitrary MeTTa programs (reductions, proofs, queries) through
    the full MeTTa runtime. Each call gets an isolated runner.

    Args:
        metta_code: MeTTa source code to execute.
        premise: Optional natural-language premise. When provided, the code
            and results are stored to the annotations parquet as a
            human-annotated ontology entry (version v0.0.4-human).

    Returns dict with ``results`` (list of result strings) or ``error``.
    """
    try:
        from hyperon import MeTTa

        runner = MeTTa()
        raw_results = runner.run(metta_code)
        # raw_results is a list of lists (one per !-expression)
        results = []
        for group in raw_results:
            results.append([str(atom) for atom in group])
    except Exception as e:
        return {"success": False, "error": str(e)}

    response: dict[str, Any] = {"success": True, "results": results}

    if premise is not None:
        annotation_id = str(uuid.uuid4())
        system_prompt = ANNOTATION_GUIDELINE_PATH.read_text()

        row = pl.DataFrame(
            [
                {
                    "annotation_id": annotation_id,
                    "index": 0,
                    "premise": premise,
                    "hypothesis": None,
                    "label": RelationKind.ONTOLOGY.value,
                    "metta_premise": metta_code.strip(),
                    "metta_hypothesis": None,
                    "generation_model": "claude-opus-4-6",
                    "system_prompt": system_prompt,
                    "version": "v0.0.4-human",
                    "is_valid": True,
                    "input_tokens": None,
                    "output_tokens": None,
                }
            ],
            schema={
                "annotation_id": pl.Utf8,
                "index": pl.UInt32,
                "premise": pl.Utf8,
                "hypothesis": pl.Utf8,
                "label": pl.Utf8,
                "metta_premise": pl.Utf8,
                "metta_hypothesis": pl.Utf8,
                "generation_model": pl.Utf8,
                "system_prompt": pl.Utf8,
                "version": pl.Utf8,
                "is_valid": pl.Boolean,
                "input_tokens": pl.Int64,
                "output_tokens": pl.Int64,
            },
        )

        try:
            if ANNOTATIONS_PATH.exists():
                existing = pl.read_parquet(ANNOTATIONS_PATH)
                combined = pl.concat([existing, row], how="diagonal_relaxed")
            else:
                ANNOTATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
                combined = row
            combined.write_parquet(ANNOTATIONS_PATH)
            logger.info(
                "Stored execute_metta annotation",
                annotation_id=annotation_id,
                premise=premise,
                path=str(ANNOTATIONS_PATH),
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
    model: str = "claude",
) -> dict[str, Any]:
    """Check whether MeTTa expressions satisfy a logical relation.

    Automatically stores the validated result to the annotations cache.

    Args:
        metta_premise: MeTTa s-expression(s) for the premise.
        metta_hypothesis: MeTTa s-expression(s) for the hypothesis.
        relation: Expected relation — one of "entailment", "contradiction", "neutral".
        premise: Original natural-language premise (stored with the annotation).
        hypothesis: Original natural-language hypothesis (stored with the annotation).
        model: Which model generated the expressions (default: "claude").

    Returns dict with ``valid`` (bool), ``message``, and ``annotation_id``.
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

    # Store the validated result
    annotation_id = str(uuid.uuid4())
    system_prompt = ANNOTATION_GUIDELINE_PATH.read_text()

    row = pl.DataFrame(
        [
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
                "version": "0.0.3",
                "is_valid": is_valid,
                "input_tokens": None,
                "output_tokens": None,
            }
        ],
        schema={
            "annotation_id": pl.Utf8,
            "index": pl.UInt32,
            "premise": pl.Utf8,
            "hypothesis": pl.Utf8,
            "label": pl.Utf8,
            "metta_premise": pl.Utf8,
            "metta_hypothesis": pl.Utf8,
            "generation_model": pl.Utf8,
            "system_prompt": pl.Utf8,
            "version": pl.Utf8,
            "is_valid": pl.Boolean,
            "input_tokens": pl.Int64,
            "output_tokens": pl.Int64,
        },
    )

    try:
        if ANNOTATIONS_PATH.exists():
            existing = pl.read_parquet(ANNOTATIONS_PATH)
            combined = pl.concat([existing, row], how="diagonal_relaxed")
        else:
            ANNOTATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
            combined = row
        combined.write_parquet(ANNOTATIONS_PATH)
        logger.info(
            "Stored validated annotation",
            annotation_id=annotation_id,
            is_valid=is_valid,
            path=str(ANNOTATIONS_PATH),
        )
    except Exception as e:
        logger.error("Failed to store annotation", error=str(e))
        return {
            "valid": is_valid,
            "message": f"Expressions {'match' if is_valid else 'do NOT match'} expected relation {label.value}. WARNING: failed to store: {e}",
        }

    return {
        "valid": is_valid,
        "message": f"Expressions {'match' if is_valid else 'do NOT match'} expected relation {label.value}.",
        "annotation_id": annotation_id,
    }


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

    # Append to annotations parquet
    row = pl.DataFrame(
        [
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
                "version": "0.0.3",
                "is_valid": is_valid,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            }
        ],
        schema={
            "annotation_id": pl.Utf8,
            "index": pl.UInt32,
            "premise": pl.Utf8,
            "hypothesis": pl.Utf8,
            "label": pl.Utf8,
            "metta_premise": pl.Utf8,
            "metta_hypothesis": pl.Utf8,
            "generation_model": pl.Utf8,
            "system_prompt": pl.Utf8,
            "version": pl.Utf8,
            "is_valid": pl.Boolean,
            "input_tokens": pl.Int64,
            "output_tokens": pl.Int64,
        },
    )

    try:
        if ANNOTATIONS_PATH.exists():
            existing = pl.read_parquet(ANNOTATIONS_PATH)
            combined = pl.concat([existing, row], how="diagonal_relaxed")
        else:
            ANNOTATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
            combined = row
        combined.write_parquet(ANNOTATIONS_PATH)
        logger.info("Appended annotation to parquet", path=str(ANNOTATIONS_PATH))
    except Exception as e:
        logger.error("Failed to append annotation", error=str(e))

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
    annotation_model: str = "openai:gpt-4o-mini",
    hf_id: str = "squad",
    filename: str = "train.parquet",
    split: str = "train",
) -> dict[str, Any]:
    """Execute the full batch annotation pipeline.

    Args:
        subset_size: Number of samples to process.
        batch_size: Batch size for concurrent processing.
        annotation_model: Model to use for annotation generation.
        hf_id: HuggingFace dataset ID.
        filename: Filename within the dataset repository.
        split: Dataset split.

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
async def run_clean_pipeline(
    hf_id: str = "JungeWerther/metta-nl-corpus-bronze-0.1",
    filename: str = "annotations.parquet",
) -> dict[str, Any]:
    """Re-validate a bronze dataset through the cleaning pipeline.

    Args:
        hf_id: HuggingFace dataset ID for the bronze dataset.
        filename: Filename within the dataset repository.

    Returns execution status and result counts.
    """
    from metta_nl_corpus.services.pipeline_executor import PipelineExecutor

    executor = PipelineExecutor()
    result = await executor.execute_clean_pipeline(
        hf_id=hf_id,
        filename=filename,
    )

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
    filter_value: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Query annotations or validations parquet files.

    Args:
        file: Which file to query — "annotations", "validations", or "cleaned".
        filter_column: Optional column name to filter on.
        filter_value: Value to match in the filter column.
        limit: Maximum number of rows to return (default 20).

    Returns matching rows as a list of dicts, plus the total count.
    """
    path_map: dict[str, Path] = {
        "annotations": ANNOTATIONS_PATH,
        "validations": VALIDATIONS_PATH,
        "cleaned": PROJECT_ROOT / "datasets" / "cleaned_annotations.parquet",
    }

    path = path_map.get(file)
    if path is None:
        return {"error": f"Unknown file '{file}'. Use: {', '.join(path_map)}"}

    if not path.exists():
        return {"error": f"File not found: {path}"}

    try:
        df = pl.read_parquet(path)
    except Exception as e:
        return {"error": f"Failed to read parquet: {e}"}

    if filter_column and filter_value:
        if filter_column not in df.columns:
            return {
                "error": f"Column '{filter_column}' not found. Available: {df.columns}",
            }
        df = df.filter(pl.col(filter_column).cast(pl.Utf8) == filter_value)

    total = len(df)
    rows = df.head(limit).to_dicts()

    # Truncate long string values to keep MCP response size manageable
    max_cell_len = 200
    for row in rows:
        for key, val in row.items():
            if isinstance(val, str) and len(val) > max_cell_len:
                row[key] = val[:max_cell_len] + "..."

    return {
        "total": total,
        "returned": len(rows),
        "columns": df.columns,
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

    if not ANNOTATIONS_PATH.exists():
        return {"error": f"Annotations file not found: {ANNOTATIONS_PATH}"}

    df = pl.read_parquet(ANNOTATIONS_PATH)
    match = df.filter(pl.col("annotation_id") == annotation_id)

    if len(match) == 0:
        return {"error": f"Annotation '{annotation_id}' not found."}

    label_str = match["label"][0]
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

    old_version = match["version"][0] or "0.0.0"
    human_version = (
        old_version if old_version.endswith("-human") else f"{old_version}-human"
    )

    # Ensure fix_reason column exists for older parquet files
    if "fix_reason" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("fix_reason"))

    df = df.with_columns(
        [
            pl.when(pl.col("annotation_id") == annotation_id)
            .then(pl.lit(metta_premise.strip()))
            .otherwise(pl.col("metta_premise"))
            .alias("metta_premise"),
            pl.when(pl.col("annotation_id") == annotation_id)
            .then(pl.lit(metta_hypothesis.strip()))
            .otherwise(pl.col("metta_hypothesis"))
            .alias("metta_hypothesis"),
            pl.when(pl.col("annotation_id") == annotation_id)
            .then(pl.lit(is_valid))
            .otherwise(pl.col("is_valid"))
            .alias("is_valid"),
            pl.when(pl.col("annotation_id") == annotation_id)
            .then(pl.lit(human_version))
            .otherwise(pl.col("version"))
            .alias("version"),
            pl.when(pl.col("annotation_id") == annotation_id)
            .then(pl.lit(fix_reason))
            .otherwise(pl.col("fix_reason"))
            .alias("fix_reason"),
        ]
    )

    df.write_parquet(ANNOTATIONS_PATH)
    logger.info(
        "Updated annotation",
        annotation_id=annotation_id,
        is_valid=is_valid,
        version=human_version,
        fix_reason=fix_reason,
    )

    updated = df.filter(pl.col("annotation_id") == annotation_id).to_dicts()[0]
    # Truncate system_prompt for response size
    if "system_prompt" in updated and isinstance(updated["system_prompt"], str):
        updated["system_prompt"] = updated["system_prompt"][:100] + "..."

    return {"success": True, "is_valid": is_valid, "annotation": updated}
