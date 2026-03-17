"""Semantic vector search over NL premises and MeTTa expressions.

Uses sentence-transformers for local embedding generation and SQLite
for vector storage. Cosine similarity via numpy dot product on
L2-normalized vectors.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import NamedTuple

import numpy as np
from structlog import get_logger

logger = get_logger(__name__)

_MODEL_NAME = "all-MiniLM-L6-v2"
_MODEL_DIM = 384

# Lazy-loaded singleton
_model = None


def _get_model():
    """Lazy-load the sentence-transformer model."""
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer

        _model = SentenceTransformer(_MODEL_NAME)
        logger.info("loaded_embedding_model", model=_MODEL_NAME, dim=_MODEL_DIM)
    return _model


def embed_texts(texts: Sequence[str]) -> np.ndarray:
    """Embed a batch of texts. Returns (N, 384) float32 array, L2-normalized."""
    model = _get_model()
    return model.encode(list(texts), normalize_embeddings=True, show_progress_bar=False)


class SearchResult(NamedTuple):
    """A single search hit."""

    annotation_id: str
    premise: str
    metta_premise: str
    score: float


def search_vectors(
    query: str,
    corpus_ids: Sequence[str],
    corpus_vecs: np.ndarray,
    top_k: int = 10,
) -> list[tuple[str, float]]:
    """Return top-K (annotation_id, score) pairs by cosine similarity.

    Assumes corpus_vecs are L2-normalized, so dot product = cosine similarity.
    """
    query_vec = embed_texts([query])[0]
    scores = corpus_vecs @ query_vec
    top_indices = np.argsort(scores)[::-1][:top_k]
    return [(corpus_ids[i], float(scores[i])) for i in top_indices]
