"""Thin Starlette HTTP API wrapping the MeTTa annotation pipeline.

Endpoints:
    POST /annotate       — single title → MeTTa expressions
    POST /batch-annotate — list of titles (max 20), sequential
    GET  /health         — readiness check
"""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel, Field
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from structlog import get_logger

from metta_nl_corpus.constants import ANNOTATION_GUIDELINE_PATH, ANNOTATIONS_DB_PATH
from metta_nl_corpus.lib.helpers import parse_all
from metta_nl_corpus.lib.storage import AnnotationStore
from metta_nl_corpus.models import RelationKind

logger = get_logger(__name__)

store = AnnotationStore(ANNOTATIONS_DB_PATH)

MAX_BATCH_SIZE = 20


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class AnnotateRequest(BaseModel):
    title: str
    model: str = "openai:gpt-4o-mini"


class AnnotateResponse(BaseModel):
    title: str
    metta_expressions: str | None = None
    is_valid: bool = False
    annotation_id: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    error: str | None = None


class BatchAnnotateRequest(BaseModel):
    titles: list[str] = Field(max_length=MAX_BATCH_SIZE)
    model: str = "openai:gpt-4o-mini"


# ---------------------------------------------------------------------------
# Core annotation logic
# ---------------------------------------------------------------------------


async def _annotate_title(title: str, model: str) -> AnnotateResponse:
    """Run the MeTTa agent on a single title (ontology label)."""
    from metta_nl_corpus.services.defs.transformation.assets import (
        ExpressionDeps,
        _create_metta_agent,
        last_generation_attempt,
    )

    system_prompt = ANNOTATION_GUIDELINE_PATH.read_text()
    agent = _create_metta_agent(system_prompt, model)
    deps = ExpressionDeps(
        premise=title,
        hypothesis="",
        label=RelationKind.ONTOLOGY,
    )
    last_generation_attempt.set(None)

    try:
        result = await agent.run(
            f"Generate MeTTa ontology expressions for: {title}",
            deps=deps,
        )
    except Exception as exc:
        return AnnotateResponse(title=title, error=str(exc))

    output = result.output
    usage = result.usage()
    metta_expressions = output.metta_premise
    input_tokens = getattr(usage, "input_tokens", None)
    output_tokens = getattr(usage, "output_tokens", None)

    # Validate syntax
    is_valid = False
    try:
        atoms = parse_all(metta_expressions)
        is_valid = len(atoms) > 0
    except Exception:
        pass

    # Persist to annotation store
    annotation_id = str(uuid.uuid4())
    try:
        store.insert_annotation(
            {
                "annotation_id": annotation_id,
                "index": 0,
                "premise": title,
                "hypothesis": None,
                "label": RelationKind.ONTOLOGY.value,
                "metta_premise": metta_expressions.strip()
                if metta_expressions
                else None,
                "metta_hypothesis": None,
                "generation_model": model,
                "system_prompt": system_prompt,
                "version": "0.0.3",
                "is_valid": is_valid,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            }
        )
    except Exception as exc:
        logger.error("Failed to store annotation", error=str(exc))

    return AnnotateResponse(
        title=title,
        metta_expressions=metta_expressions,
        is_valid=is_valid,
        annotation_id=annotation_id,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


async def health(_request: Request) -> Response:
    return JSONResponse({"status": "ok"})


async def annotate(request: Request) -> Response:
    body = await request.json()
    req = AnnotateRequest.model_validate(body)
    result = await _annotate_title(req.title, req.model)
    return JSONResponse(result.model_dump())


async def batch_annotate(request: Request) -> Response:
    body = await request.json()
    req = BatchAnnotateRequest.model_validate(body)

    if len(req.titles) > MAX_BATCH_SIZE:
        return JSONResponse(
            {"error": f"Maximum {MAX_BATCH_SIZE} titles per batch"},
            status_code=400,
        )

    results: list[dict[str, Any]] = []
    for title in req.titles:
        result = await _annotate_title(title, req.model)
        results.append(result.model_dump())

    return JSONResponse(results)


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app() -> Starlette:
    return Starlette(
        routes=[
            Route("/health", health, methods=["GET"]),
            Route("/annotate", annotate, methods=["POST"]),
            Route("/batch-annotate", batch_annotate, methods=["POST"]),
        ],
    )
