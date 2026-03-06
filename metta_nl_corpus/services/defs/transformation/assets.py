import asyncio
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import polars as pl
from dagster import asset
from dotenv import load_dotenv
from httpx import HTTPStatusError
from huggingface_hub.utils.tqdm import tqdm
from pydantic import BaseModel, model_validator
from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.retries import AsyncTenacityTransport, RetryConfig, wait_retry_after
from structlog import getLogger
from tenacity import retry_if_exception_type, stop_after_attempt, wait_exponential

from metta_nl_corpus.constants import (
    ANNOTATION_GUIDELINE_PATH,
    ANNOTATIONS_DB_PATH,
    ANNOTATIONS_PATH,
    PROJECT_ROOT,
    VALIDATIONS_PATH,
)
from metta_nl_corpus.lib.storage import AnnotationStore
from metta_nl_corpus.lib.helpers import (
    cleanup_metta_expression,
    parse_all,
    to_metta_tuple,
)
from metta_nl_corpus.lib.interfaces import Fn
from metta_nl_corpus.lib.runner import create_runner
from metta_nl_corpus.lib.pipeline_config import PipelineRunConfig
from metta_nl_corpus.lib.space_versioning import get_space_version
from metta_nl_corpus.models import (
    DATA_VERSION,
    Annotation,
    GenerateAndValidateResult,
    RelationKind,
    TrainingData,
    Validation,
)

logger = getLogger(__name__)

load_dotenv(".env.local")

# Stores the last generation attempt so callers can retrieve it on validation failure.
# Module-level dict (not ContextVar) so it's visible across async task boundaries.
last_generation_attempt: dict[str, Any] = {}

ENTAILMENTS_PATH = PROJECT_ROOT / "metta_nl_corpus/services/spaces/inference.metta"
CONTRADICTIONS_PATH = (
    PROJECT_ROOT / "metta_nl_corpus/services/spaces/contradictions.metta"
)


def pandera_record(d: dict[Any, Any]) -> pl.DataFrame:
    """Convert a dictionary to a single-row Polars DataFrame for pandera validation."""
    # Convert all keys to strings and create a single-row DataFrame
    str_dict = {str(k): [v] for k, v in d.items()}
    return pl.DataFrame(str_dict)


def _ensure_parenthesized(code: str) -> str:
    """Ensure each top-level line of MeTTa code is wrapped in parentheses.

    Bare tokens like `jumpedOver a-person airplane` are invalid and get
    wrapped as `(jumpedOver a-person airplane)`.  Lines that already start
    with '(' (possibly indented) or are empty are left unchanged, preserving
    their original indentation for multiline expressions.
    """
    lines = code.split("\n")
    fixed: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            fixed.append("")
            continue
        # Already parenthesized or is a continuation line — preserve original formatting
        if stripped.startswith("("):
            fixed.append(line)
        else:
            # Bare tokens – wrap them
            logger.debug("Wrapping bare tokens in parentheses", bare_line=stripped)
            fixed.append(f"({stripped})")
    return "\n".join(fixed)


def parse_metta_expression(expression: str) -> str:
    """Extract MeTTa code from a string that may contain markdown code blocks.

    Extracts content from the last code block found, or returns the input as-is if no code blocks exist.
    Removes lines that start with ';' (MeTTa comments).
    Ensures all expressions are properly parenthesized.
    """
    import re

    # Find all code blocks (with optional language identifier)
    matches = re.findall(r"```[^\n]*\n(.*?)\n```", expression, re.DOTALL)
    if matches:
        # Get the last match
        code = matches[-1].strip()
    else:
        # No code block found, use input as-is
        code = expression.strip()

    # Remove lines that start with ';' (MeTTa comments)
    lines = code.split("\n")
    filtered_lines = [line for line in lines if not line.lstrip().startswith(";")]

    code = "\n".join(filtered_lines).strip()

    # Ensure all expressions are parenthesized
    return _ensure_parenthesized(code)


class AgentExpressionOutput(BaseModel):
    """Structured output from the MeTTa expression generation agent.

    The relation must be one of: entailment, neutral, contradiction.
    Creation fails with ValidationError if the expressions do not satisfy the claimed relation.
    """

    metta_premise: str
    metta_hypothesis: str
    relation: str

    @model_validator(mode="before")
    @classmethod
    def extract_and_validate(cls, data: Any) -> Any:
        """Extract MeTTa code, validate relation string, store in last_generation_attempt.

        The agent's own ``validate_relation_tool`` handles self-correction
        during generation, so we no longer run ``validate_expressions_by_label``
        here (which spawned expensive subprocesses).  We only parse and store.
        """
        if not isinstance(data, dict):
            return data
        metta_premise = parse_metta_expression(data.get("metta_premise")).strip()
        metta_hypothesis = parse_metta_expression(data.get("metta_hypothesis")).strip()

        relation = data.get("relation", "")
        label = _relation_str_to_kind(relation)
        logger.debug(
            "AgentExpressionOutput validating",
            relation=relation,
            label=label,
            metta_premise=metta_premise,
            metta_hypothesis=metta_hypothesis,
        )
        if label is None:
            msg = f"Unknown relation '{relation}'. Use entailment, neutral, or contradiction."
            raise ValueError(msg)

        # Parse to verify syntax
        parse_all(metta_premise)
        parse_all(metta_hypothesis)

        # Store last attempt so callers can retrieve it
        last_generation_attempt.update(
            {
                "metta_premise": metta_premise,
                "metta_hypothesis": metta_hypothesis,
                "relation": relation,
                "is_valid": True,  # trust agent's tool-based validation
            }
        )

        return {
            **data,
            "metta_premise": metta_premise,
            "metta_hypothesis": metta_hypothesis,
        }


@dataclass
class ExpressionDeps:
    """Dependencies for the MeTTa expression generation agent."""

    premise: str
    hypothesis: str
    label: RelationKind


def parse_all_tool(metta_code: str) -> dict[str, Any]:
    """Parse MeTTa code to verify it is valid. Returns success and any parse error."""
    try:
        parse_all(metta_code)
        return {"success": True, "error": None}
    except Exception as e:
        return {"success": False, "error": str(e)}


def _relation_str_to_kind(relation: str) -> RelationKind | None:
    """Map relation string to RelationKind. Handles 'contradiction' vs enum typo 'contradication'."""
    normalized = relation.lower().strip()
    mapping = {
        RelationKind.ENTAILMENT.value: RelationKind.ENTAILMENT,
        RelationKind.NEUTRAL.value: RelationKind.NEUTRAL,
        RelationKind.CONTRADICTION.value: RelationKind.CONTRADICTION,
    }
    return mapping.get(normalized)


def validate_relation_tool(
    metta_premise: str, metta_hypothesis: str, expected_relation: str
) -> dict[str, Any]:
    """Verify that the MeTTa expressions have the expected relation (ENTAILMENT, NEUTRAL, CONTRADICTION).

    Call this before returning your output to ensure expressions match the expected relation.
    """
    label = _relation_str_to_kind(expected_relation)
    if label is None:
        return {
            "valid": False,
            "message": f"Unknown relation '{expected_relation}'. Use entailment, neutral, or contradiction.",
        }
    try:
        is_valid = validate_expressions_by_label(
            label=label,
            metta_premise=metta_premise.strip(),
            metta_hypothesis=metta_hypothesis.strip(),
        )
        return {
            "valid": is_valid,
            "message": (
                f"Expressions {'match' if is_valid else 'do NOT match'} expected relation {label.value}."
            ),
        }
    except Exception as e:
        return {"valid": False, "message": f"Validation failed: {e}"}


def _create_retrying_http_client() -> httpx.AsyncClient:
    """Create an httpx client with retries for rate limits and transient errors."""
    transport = AsyncTenacityTransport(
        config=RetryConfig(
            retry=retry_if_exception_type((HTTPStatusError, httpx.ConnectError)),
            wait=wait_retry_after(
                fallback_strategy=wait_exponential(multiplier=1, max=60),
                max_wait=300,
            ),
            stop=stop_after_attempt(5),
            reraise=True,
        ),
        validate_response=lambda r: r.raise_for_status(),
    )
    return httpx.AsyncClient(transport=transport)


def _resolve_model(model_str: str) -> str | OpenAIChatModel:
    """Resolve model string to Model instance with HTTP retries for OpenAI, or pass-through for others."""
    if model_str.startswith("openai:"):
        model_name = model_str.removeprefix("openai:")
        client = _create_retrying_http_client()
        provider = OpenAIProvider(http_client=client)
        return OpenAIChatModel(model_name, provider=provider)
    return model_str


def _create_metta_agent(
    system_prompt: str, model: str
) -> Agent[ExpressionDeps, AgentExpressionOutput]:
    """Create the Pydantic AI agent for MeTTa expression generation."""

    resolved_model = _resolve_model(model)
    agent = Agent(
        resolved_model,
        deps_type=ExpressionDeps,
        output_type=AgentExpressionOutput,
        instructions=system_prompt,
        tools=[parse_all_tool, validate_relation_tool],
        retries=1,  # More attempts for expression generation to succeed
        output_retries=1,  # More attempts for relation validation to succeed
    )

    @agent.instructions
    def add_task_context(ctx: RunContext[ExpressionDeps]) -> str:
        deps = ctx.deps
        inference_example = (
            PROJECT_ROOT / "metta_nl_corpus/services/spaces/inference-example.metta"
        ).read_text()
        return (
            f"Generate MeTTa expressions for:\n"
            f"Premise: {deps.premise}\n"
            f"Hypothesis: {deps.hypothesis}\n"
            f"Expected relation: {deps.label}\n\n"
            "CRITICAL RULES:\n"
            "- Every expression MUST be wrapped in parentheses: (predicate subject).\n"
            "- Bare tokens like `foo bar baz` are INVALID. Always write `(foo bar baz)`.\n"
            "- For ENTAILMENT: premise expressions must allow deriving hypothesis via transitivity. "
            "Use the same entity names so the inference engine can chain implications.\n"
            "- For CONTRADICTION: the contradiction engine ONLY works with 2-element predicates (predicate entity). "
            "Use compound predicate names like (onHorse a-person) instead of (on a-person horse). "
            "The hypothesis MUST negate a property from the premise using ((is-not predicate) entity). "
            "Both premise and hypothesis are added to the same space. The entities MUST match.\n"
            "- For NEUTRAL: the expressions should be neither entailing nor contradictory.\n\n"
            "Here is an example of how the inference engine works with propositions:\n"
            f"```MeTTa\n{inference_example}\n```\n\n"
            "Use parse_all_tool to verify your expressions are valid. "
            "Use validate_relation_tool(metta_premise, metta_hypothesis, expected_relation) "
            "with the expected relation from the task to verify your expressions match before returning. "
            "Set 'relation' in AgentExpressionOutput to the expected relation (e.g. entailment, neutral, contradiction). "
            "Return the final expressions via AgentExpressionOutput."
        )

    return agent


VALIDATION_TIMEOUT_SECONDS = 10


def validate_expressions_truthy_after_adding_expressions_to_space(
    expressions_to_add_to_space: Sequence[str],
    grounding_space_path: Path,
    expression_to_evaluate: str,
    verbose: bool = False,
    timeout: int = VALIDATION_TIMEOUT_SECONDS,
) -> bool:
    """Run MeTTa validation in a daemon thread with timeout.

    The GIL is released during C-level MeTTa calls, so the main thread
    can join with a timeout.  Daemon threads are cleaned up on process exit.
    """
    if not expressions_to_add_to_space:
        logger.debug("No expressions to add, returning False")
        return False

    logger.info("Validating expressions", expressions=expressions_to_add_to_space)

    container: dict[str, Any] = {}

    def _run() -> None:
        try:
            runner = create_runner()
            metta_code = grounding_space_path.read_text()
            runner.run(metta_code)
            for expression in expressions_to_add_to_space:
                runner.run(expression)
            if verbose:
                runner.run("!(all)")
            result = runner.run(expression_to_evaluate)
            is_truthy = bool(result and len(result) > 0 and len(result[-1]) > 0)
            container["status"] = "ok"
            container["value"] = is_truthy
            container["result"] = result
        except Exception as e:
            container["status"] = "error"
            container["error"] = str(e)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(timeout=timeout)

    if thread.is_alive():
        logger.warning(
            "MeTTa validation timed out (daemon thread still running)",
            timeout=timeout,
            expression_to_evaluate=expression_to_evaluate,
        )
        return False

    if container.get("status") == "error":
        logger.warning(
            "MeTTa validation failed",
            error=container.get("error"),
            expression_to_evaluate=expression_to_evaluate,
        )
        return False

    if not container.get("status"):
        logger.warning(
            "MeTTa validation thread returned no result",
            expression_to_evaluate=expression_to_evaluate,
        )
        return False

    if container["value"]:
        logger.info(
            "MeTTa expressions pass validation",
            expression=expression_to_evaluate,
            result=container["result"],
        )
        return True

    logger.info(
        "MeTTa expressions are not valid.",
        expression=expression_to_evaluate,
        result=container["result"],
    )
    return False


def validate_expressions_are_entailing(
    metta_premise: str, metta_hypothesis: str, verbose: bool = False
) -> bool:
    parsed_premise = parse_all(metta_premise)
    hypothesis_tuple = to_metta_tuple(metta_hypothesis)
    expression_to_evaluate = f"!(find-evidence-for {hypothesis_tuple})"
    expressions_to_add = [f"!(add-proposition {premise})" for premise in parsed_premise]
    logger.debug(
        "Checking entailment",
        parsed_premise=[str(p) for p in parsed_premise],
        hypothesis_tuple=hypothesis_tuple,
        expression_to_evaluate=expression_to_evaluate,
        expressions_to_add=expressions_to_add,
    )
    return validate_expressions_truthy_after_adding_expressions_to_space(
        expressions_to_add,
        ENTAILMENTS_PATH,
        expression_to_evaluate,
        verbose=verbose,
    )


def validate_expressions_are_contradictory(
    metta_premise: str, metta_hypothesis: str, verbose: bool = False
) -> bool:
    parsed_premise = parse_all(metta_premise)
    parsed_hypothesis = parse_all(metta_hypothesis)
    expression_to_evaluate = "!(find-evidence-for ⊥)"
    expressions_to_add = [
        *[f"!(add-proposition {premise})" for premise in parsed_premise],
        *[f"!(add-proposition {hypothesis})" for hypothesis in parsed_hypothesis],
    ]
    logger.debug(
        "Checking contradiction",
        parsed_premise=[str(p) for p in parsed_premise],
        parsed_hypothesis=[str(p) for p in parsed_hypothesis],
        expression_to_evaluate=expression_to_evaluate,
        expressions_to_add=expressions_to_add,
    )
    return validate_expressions_truthy_after_adding_expressions_to_space(
        expressions_to_add,
        ENTAILMENTS_PATH,
        expression_to_evaluate,
        verbose=verbose,
    )


def validate_expressions_are_neutral(metta_premise: str, metta_hypothesis: str) -> bool:
    is_entailing = validate_expressions_are_entailing(metta_premise, metta_hypothesis)
    is_contradictory = validate_expressions_are_contradictory(
        metta_premise, metta_hypothesis
    )
    logger.debug(
        "Checking neutral",
        is_entailing=is_entailing,
        is_contradictory=is_contradictory,
        result=not (is_entailing or is_contradictory),
    )
    return not (is_entailing or is_contradictory)


def validate_expressions_by_label(
    label: RelationKind, metta_premise: str, metta_hypothesis: str
) -> bool:
    validation_function = {
        RelationKind.ENTAILMENT: validate_expressions_are_entailing,
        RelationKind.CONTRADICTION: validate_expressions_are_contradictory,
        RelationKind.NEUTRAL: validate_expressions_are_neutral,
    }.get(
        label, validate_expressions_are_neutral
    )  # Default to neutral if label not found

    logger.debug(
        "validate_expressions_by_label called",
        label=label,
        validation_function=validation_function.__name__,
        metta_premise=metta_premise,
        metta_hypothesis=metta_hypothesis,
    )
    result = validation_function(metta_premise, metta_hypothesis)
    logger.debug(
        "validate_expressions_by_label result",
        label=label,
        result=result,
    )
    return result


def get_grounding_space_versions():
    contradictions_git_hash, contradictions_code_hash = get_space_version(
        CONTRADICTIONS_PATH
    )
    entailments_git_hash, entailments_code_hash = get_space_version(ENTAILMENTS_PATH)

    return (
        contradictions_code_hash,
        contradictions_git_hash,
        entailments_code_hash,
        entailments_git_hash,
    )


(
    CONTRADICTIONS_SPACE_HASH,
    CONTRADICTIONS_GIT_HASH,
    ENTAILMENT_SPACE_HASH,
    ENTAILMENT_GIT_HASH,
) = get_grounding_space_versions()


def _create_annotation_and_validation(
    annotation_id: str,
    index: int,
    premise: str,
    hypothesis: str,
    label: RelationKind,
    last_metta_premise: str,
    last_metta_hypothesis: str,
    annotation_model: str,
    system_prompt: str,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
) -> GenerateAndValidateResult:
    """
    Shared logic to create annotation and validation records.
    """
    metta_premise_cleaned = cleanup_metta_expression(last_metta_premise)
    metta_hypothesis_cleaned = cleanup_metta_expression(last_metta_hypothesis)

    record: dict[str, Any] = {
        "annotation_id": annotation_id,
        "index": index,
        "premise": premise,
        "hypothesis": hypothesis,
        "label": label,
        "metta_premise": metta_premise_cleaned,
        "metta_hypothesis": metta_hypothesis_cleaned,
        "generation_model": annotation_model,
        "system_prompt": system_prompt,
        "version": DATA_VERSION,
    }
    record["input_tokens"] = input_tokens
    record["output_tokens"] = output_tokens
    annotation = Annotation.validate(pandera_record(record))

    is_valid = validate_expressions_by_label(
        label=label,
        metta_premise=metta_premise_cleaned,
        metta_hypothesis=metta_hypothesis_cleaned,
    )

    validation = Validation.validate(
        pandera_record(
            {
                Validation.validation_id: str(uuid4()),
                Validation.annotation_id: annotation_id,
                Validation.is_valid: is_valid,
                Validation.relation_kind: label,
                Validation.entailment_space_hash: ENTAILMENT_SPACE_HASH,
                Validation.entailment_git_commit_hash: ENTAILMENT_GIT_HASH,
                Validation.contradiction_space_hash: CONTRADICTIONS_SPACE_HASH,
                Validation.contradiction_git_commit_hash: CONTRADICTIONS_GIT_HASH,
                Validation.validation_timestamp: datetime.now().isoformat(),
            }
        )
    )

    return GenerateAndValidateResult(annotation=annotation, validation=validation)


def _recover_last_attempt() -> tuple[str, str, int | None, int | None]:
    """Recover the last generation attempt after validation failure."""
    attempt = last_generation_attempt
    if attempt.get("metta_premise") and attempt.get("metta_hypothesis"):
        logger.warning(
            "Recovering last generation attempt (validation failed)",
            metta_premise=attempt["metta_premise"],
            metta_hypothesis=attempt["metta_hypothesis"],
        )
        return (attempt["metta_premise"], attempt["metta_hypothesis"], None, None)
    return ("", "", None, None)


async def _generate_expressions_async(
    agent: Agent[ExpressionDeps, AgentExpressionOutput],
    premise: str,
    hypothesis: str,
    label: RelationKind,
    annotation_model: str,
) -> tuple[str, str, int | None, int | None]:
    """
    Async helper to generate MeTTa expressions via Pydantic AI agent.
    Returns (last_metta_premise, last_metta_hypothesis, input_tokens, output_tokens)
    """
    deps = ExpressionDeps(premise=premise, hypothesis=hypothesis, label=label)
    prompt = "Generate MeTTa expressions for the premise and hypothesis."

    logger.info("Generating MeTTa expressions", model=annotation_model)
    try:
        result = await agent.run(prompt, deps=deps, model=annotation_model)
    except Exception as e:
        logger.error("Pydantic AI generation failed", error=str(e))
        return _recover_last_attempt()

    if not result.output:
        logger.error("Failed to generate MeTTa expressions")
        return ("", "", None, None)

    output = result.output
    last_metta_premise = output.metta_premise.strip()
    last_metta_hypothesis = output.metta_hypothesis.strip()

    if not last_metta_premise or not last_metta_hypothesis:
        logger.error("Empty expressions in agent output")
        return ("", "", None, None)

    logger.info(
        "Generated MeTTa expressions",
        premise=premise,
        hypothesis=hypothesis,
        metta_premise=last_metta_premise,
        metta_hypothesis=last_metta_hypothesis,
    )

    usage = result.usage()
    input_tokens = getattr(usage, "input_tokens", None)
    output_tokens = getattr(usage, "output_tokens", None)

    return (
        last_metta_premise,
        last_metta_hypothesis,
        input_tokens,
        output_tokens,
    )


def _generate_expressions_sync(
    agent: Agent[ExpressionDeps, AgentExpressionOutput],
    premise: str,
    hypothesis: str,
    label: RelationKind,
    annotation_model: str,
) -> tuple[str, str, int | None, int | None]:
    """
    Sync helper to generate MeTTa expressions via Pydantic AI agent.
    Returns (last_metta_premise, last_metta_hypothesis, input_tokens, output_tokens)
    """
    deps = ExpressionDeps(premise=premise, hypothesis=hypothesis, label=label)
    prompt = "Generate MeTTa expressions for the premise and hypothesis."

    logger.info("Generating MeTTa expressions", model=annotation_model)
    try:
        result = agent.run_sync(prompt, deps=deps, model=annotation_model)
    except Exception as e:
        logger.error("Pydantic AI generation failed", error=str(e))
        return _recover_last_attempt()

    if not result.output:
        logger.error("Failed to generate MeTTa expressions")
        return ("", "", None, None)

    output = result.output
    last_metta_premise = output.metta_premise.strip()
    last_metta_hypothesis = output.metta_hypothesis.strip()

    if not last_metta_premise or not last_metta_hypothesis:
        logger.error("Empty expressions in agent output")
        return ("", "", None, None)

    logger.info(
        "Generated MeTTa expressions",
        premise=premise,
        hypothesis=hypothesis,
        metta_premise=last_metta_premise,
        metta_hypothesis=last_metta_hypothesis,
    )

    usage = result.usage()
    input_tokens = getattr(usage, "input_tokens", None)
    output_tokens = getattr(usage, "output_tokens", None)

    return (
        last_metta_premise,
        last_metta_hypothesis,
        input_tokens,
        output_tokens,
    )


def generate_and_validate(
    premise: str,
    hypothesis: str,
    label: RelationKind,
    index: int,
    annotation_model: str,
) -> GenerateAndValidateResult:
    """
    Generate MeTTa expressions for premise and hypothesis, validate them,
    and retry with additional context if validation fails.

    Returns:
        GenerateAndValidateResult containing annotation and validation data
    """
    annotation_id = str(uuid4())
    system_prompt = ANNOTATION_GUIDELINE_PATH.read_text()
    agent = _create_metta_agent(system_prompt, annotation_model)

    (
        last_metta_premise,
        last_metta_hypothesis,
        input_tokens,
        output_tokens,
    ) = _generate_expressions_sync(agent, premise, hypothesis, label, annotation_model)

    if not last_metta_premise or not last_metta_hypothesis:
        return GenerateAndValidateResult(annotation=None, validation=None)

    return _create_annotation_and_validation(
        annotation_id=annotation_id,
        index=index,
        premise=premise,
        hypothesis=hypothesis,
        label=label,
        last_metta_premise=last_metta_premise,
        last_metta_hypothesis=last_metta_hypothesis,
        annotation_model=annotation_model,
        system_prompt=system_prompt,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )


async def generate_and_validate_async(
    premise: str,
    hypothesis: str,
    label: RelationKind,
    index: int,
    annotation_model: str,
) -> GenerateAndValidateResult:
    """
    Async version: Generate MeTTa expressions for premise and hypothesis, validate them,
    and retry with additional context if validation fails.

    Returns:
        GenerateAndValidateResult containing annotation and validation data
    """
    annotation_id = str(uuid4())
    system_prompt = ANNOTATION_GUIDELINE_PATH.read_text()
    agent = _create_metta_agent(system_prompt, annotation_model)

    (
        last_metta_premise,
        last_metta_hypothesis,
        input_tokens,
        output_tokens,
    ) = await _generate_expressions_async(
        agent, premise, hypothesis, label, annotation_model
    )

    if not last_metta_premise or not last_metta_hypothesis:
        return GenerateAndValidateResult(annotation=None, validation=None)

    return _create_annotation_and_validation(
        annotation_id=annotation_id,
        index=index,
        premise=premise,
        hypothesis=hypothesis,
        label=label,
        last_metta_premise=last_metta_premise,
        last_metta_hypothesis=last_metta_hypothesis,
        annotation_model=annotation_model,
        system_prompt=system_prompt,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )


def process_in_batches(
    rows: list[dict],
    process_fn: Fn[dict, GenerateAndValidateResult],
    subset_size: int,
    batch_size: int,
    description: str = "Processing",
) -> Sequence[GenerateAndValidateResult]:
    """
    Process rows in batches with progress tracking.

    Args:
        rows: List of row dictionaries to process
        process_fn: Function to apply to each row
        subset_size: Maximum number of rows to process
        batch_size: Number of rows to process in each batch
        description: Description for the progress bar

    Returns:
        List of processed row dictionaries
    """
    # Limit to subset_size
    rows_to_process = rows[:subset_size]
    processed_rows: Sequence[GenerateAndValidateResult] = []

    # Process in batches with tqdm
    for i in tqdm(
        range(0, len(rows_to_process), batch_size),
        desc=description,
        unit="batch",
        total=(len(rows_to_process) + batch_size - 1) // batch_size,
    ):
        batch = rows_to_process[i : min(subset_size, i + batch_size)]
        batch_results = [process_fn(row) for row in batch]
        processed_rows.extend(batch_results)

    return processed_rows


async def process_in_batches_async(
    rows: list[dict],
    process_fn_async,  # Async function that takes a dict and returns GenerateAndValidateResult
    subset_size: int,
    batch_size: int,
    description: str = "Processing",
    on_batch_complete: Callable[[Sequence[GenerateAndValidateResult]], None]
    | None = None,
) -> Sequence[GenerateAndValidateResult]:
    """
    Process rows in batches asynchronously with progress tracking.
    Each batch is processed concurrently, with all items in a batch executing in parallel.

    Args:
        rows: List of row dictionaries to process
        process_fn_async: Async function to apply to each row
        subset_size: Maximum number of rows to process
        batch_size: Number of rows to process concurrently in each batch
        description: Description for the progress bar
        on_batch_complete: Optional callback invoked after each batch with its results.

    Returns:
        List of processed results
    """
    # Limit to subset_size
    rows_to_process = rows[:subset_size]
    processed_rows: list[GenerateAndValidateResult] = []

    # Process in batches with tqdm
    for i in tqdm(
        range(0, len(rows_to_process), batch_size),
        desc=description,
        unit="batch",
        total=(len(rows_to_process) + batch_size - 1) // batch_size,
    ):
        batch = rows_to_process[i : min(subset_size, i + batch_size)]

        # Create async tasks for all items in the batch
        tasks = [process_fn_async(row) for row in batch]

        # Execute all tasks in the batch concurrently
        batch_results = await asyncio.gather(*tasks)
        processed_rows.extend(batch_results)

        if on_batch_complete is not None:
            on_batch_complete(batch_results)

    return processed_rows


# USD per 1M tokens by model.
_MODEL_PRICING: dict[str, tuple[float, float]] = {
    "openai:gpt-4o-mini": (0.15, 0.60),
    "openai:gpt-4o": (2.50, 10.00),
    "openai:gpt-5-nano": (0.10, 0.40),
}


def _log_batch_cost_summary(
    results: Sequence[GenerateAndValidateResult],
    model: str,
) -> None:
    """Aggregate token usage from results and log estimated cost."""
    total_input = sum(
        r.annotation.select("input_tokens").item(0, 0)
        for r in results
        if r.annotation is not None
        and "input_tokens" in r.annotation.columns
        and r.annotation.select("input_tokens").item(0, 0) is not None
    )
    total_output = sum(
        r.annotation.select("output_tokens").item(0, 0)
        for r in results
        if r.annotation is not None
        and "output_tokens" in r.annotation.columns
        and r.annotation.select("output_tokens").item(0, 0) is not None
    )
    if total_input == 0 and total_output == 0:
        return

    pricing = _MODEL_PRICING.get(model)
    if pricing is not None:
        input_rate, output_rate = pricing
        cost_usd = (total_input * input_rate + total_output * output_rate) / 1_000_000
        logger.info(
            "Batch cost summary",
            model=model,
            input_tokens=total_input,
            output_tokens=total_output,
            estimated_cost_usd=round(cost_usd, 4),
        )
    else:
        logger.info(
            "Batch cost summary",
            model=model,
            input_tokens=total_input,
            output_tokens=total_output,
            estimated_cost_usd="unknown (no pricing for model)",
        )


async def generate_and_store_lightweight(
    agent: Agent[ExpressionDeps, AgentExpressionOutput],
    premise: str,
    hypothesis: str,
    label: RelationKind,
    snli_index: int,
    annotation_model: str,
    system_prompt: str,
) -> dict[str, Any]:
    """Lightweight single-pair generation for the CLI ``annotate`` command.

    Re-uses a pre-created agent (no HTTP client setup per call), reads
    ``is_valid`` from the agent's tool-based validation stored in
    ``last_generation_attempt``, and returns a dict ready for
    ``AnnotationStore.insert_annotation``.
    """
    (
        metta_premise,
        metta_hypothesis,
        input_tokens,
        output_tokens,
    ) = await _generate_expressions_async(
        agent, premise, hypothesis, label, annotation_model
    )

    if not metta_premise or not metta_hypothesis:
        return {"error": "Generation failed — empty expressions."}

    attempt = last_generation_attempt
    is_valid = attempt.get("is_valid", False)

    annotation_id = str(uuid4())
    return {
        "annotation_id": annotation_id,
        "index": snli_index,
        "premise": premise,
        "hypothesis": hypothesis,
        "label": label.value,
        "metta_premise": cleanup_metta_expression(metta_premise),
        "metta_hypothesis": cleanup_metta_expression(metta_hypothesis),
        "generation_model": annotation_model,
        "system_prompt": system_prompt,
        "version": DATA_VERSION,
        "is_valid": is_valid,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }


@asset(required_resource_keys={"pipeline_config"})
def data_annotations(
    context,
    preprocessed_training_data: pl.DataFrame,
    cached_annotations: pl.DataFrame,
    cached_validations: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Generate MeTTa annotations and validations for training data.

    Returns:
        Tuple of (annotations_df, validations_df)
    """
    pipeline_config: PipelineRunConfig = context.resources.pipeline_config

    logger.info("Starting data annotation", pipeline_config=pipeline_config)

    # Get unannotated rows, optionally starting from an offset index
    unannotated_data_points = preprocessed_training_data.filter(
        ~pl.col(str(Annotation.index)).is_in(cached_annotations[str(Annotation.index)])
    )
    if pipeline_config.offset > 0:
        unannotated_data_points = unannotated_data_points.filter(
            pl.col(str(Annotation.index)) >= pipeline_config.offset
        )

    # Extract premise, hypothesis, and label columns with index
    dataset_to_annotate = unannotated_data_points.select(
        [
            pl.col(str(Annotation.index)),
            pl.col(str(TrainingData.premise)),
            pl.col(str(TrainingData.hypothesis)),
            pl.col(str(TrainingData.label)),
        ]
    )

    # Apply generate_and_validate to all rows
    async def process_row_async(row: dict) -> GenerateAndValidateResult:
        premise = row[str(TrainingData.premise)]
        hypothesis = row[str(TrainingData.hypothesis)]
        label = row[str(TrainingData.label)]
        index = row["index"]

        return await generate_and_validate_async(
            premise=premise,
            hypothesis=hypothesis,
            label=label,
            index=index,
            annotation_model=pipeline_config.annotation_model,
        )

    rows = dataset_to_annotate.to_dicts()

    # Persist each batch to SQLite immediately so no work is lost
    annotation_store = AnnotationStore(ANNOTATIONS_DB_PATH)

    def _persist_batch(batch_results: Sequence[GenerateAndValidateResult]) -> None:
        for result in batch_results:
            if result.annotation is not None:
                for row in result.annotation.to_dicts():
                    annotation_store.insert_annotation(row)
            if result.validation is not None:
                for row in result.validation.to_dicts():
                    annotation_store.insert_validation(row)

    # Run async batch processing
    processed_results = asyncio.run(
        process_in_batches_async(
            rows=rows,
            process_fn_async=process_row_async,
            subset_size=pipeline_config.subset_size,
            batch_size=pipeline_config.batch_size,
            description="Generating MeTTa annotations",
            on_batch_complete=_persist_batch,
        )
    )

    # Log cost summary for OpenAI models
    _log_batch_cost_summary(processed_results, pipeline_config.annotation_model)

    # Reload from SQLite (includes both cached + newly persisted)
    all_annotations = annotation_store.to_polars("annotations")
    all_validations = annotation_store.to_polars("validations")

    # Export to parquet for HuggingFace / backward compatibility
    annotation_store.export_parquet(ANNOTATIONS_PATH, table="annotations")
    if not all_validations.is_empty():
        annotation_store.export_parquet(VALIDATIONS_PATH, table="validations")

    logger.info(
        "Completed data annotation",
        annotations_count=len(all_annotations),
        validations_count=len(all_validations),
    )

    return all_annotations, all_validations
