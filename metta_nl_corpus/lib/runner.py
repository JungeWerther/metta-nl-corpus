"""MeTTa execution backend adapter.

Provides a unified interface for running MeTTa code via either the
hyperon-experimental runtime, the PeTTa subprocess transpiler, or the
JanusPeTTa in-process backend (PeTTa via janus-swi).
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from enum import StrEnum
from typing import Protocol

from structlog import get_logger

logger = get_logger(__name__)

# MeTTa symbols that clash with Prolog operators.
# Mapped to Prolog-safe alphanumeric names for serialization,
# then restored in output via deserialization.
_METTA_TO_PROLOG: Sequence[tuple[str, str]] = (
    ("=>", "__metta_implies__"),
    (">=", "__metta_gte__"),
    ("<=", "__metta_lte__"),
    (">", "__metta_gt__"),
    ("<", "__metta_lt__"),
    ("≞", "__metta_approx__"),
    ("⊥", "__metta_bottom__"),
)

_PROLOG_TO_METTA: Sequence[tuple[str, str]] = tuple(
    (prolog, metta) for metta, prolog in _METTA_TO_PROLOG
)


def _serialize_for_petta(code: str) -> str:
    """Replace MeTTa operator symbols with Prolog-safe names."""
    for metta_sym, prolog_sym in _METTA_TO_PROLOG:
        code = code.replace(metta_sym, prolog_sym)
    return code


def _deserialize_from_petta(text: str) -> str:
    """Restore Prolog-safe names back to MeTTa operator symbols."""
    for prolog_sym, metta_sym in _PROLOG_TO_METTA:
        text = text.replace(prolog_sym, metta_sym)
    return text


def _default_petta_path() -> str:
    """Resolve PeTTa path from env or standard location."""
    path = os.environ.get("PETTA_PATH")
    if path:
        return path
    candidate = os.path.expanduser("~/sites/PeTTa")
    if os.path.isdir(candidate):
        return candidate
    msg = "PETTA_PATH env var is required when METTA_BACKEND=petta (or ~/sites/PeTTa must exist)"
    raise ValueError(msg)


class MeTTaBackend(StrEnum):
    HYPERON = "hyperon"
    PETTA = "petta"
    JANUS = "janus"


class MeTTaRunner(Protocol):
    """Minimal interface for executing MeTTa code."""

    def run(self, code: str) -> Sequence[Sequence[str]]: ...


class HyperonRunner:
    """Wraps hyperon.MeTTa, converting Atom results to strings."""

    def __init__(self) -> None:
        from hyperon import MeTTa

        self._runner = MeTTa()

    def run(self, code: str) -> Sequence[Sequence[str]]:
        raw: Sequence[Sequence[object]] = self._runner.run(code)
        return [[str(atom) for atom in group] for group in raw]


class PeTTaRunner:
    """Accumulates MeTTa code and executes via PeTTa's run.sh.

    Each call to ``run`` appends code to an internal buffer and re-executes
    the full buffer through PeTTa, mirroring the stateful behaviour of the
    hyperon runner.  Only the results of the last ``!``-expression matter
    for validation, so full re-execution is correct.
    """

    def __init__(self, petta_path: str) -> None:
        self._petta_path: str = petta_path
        self._buffer: list[str] = []

    def run(self, code: str) -> Sequence[Sequence[str]]:
        self._buffer.append(code)
        full_code: str = _serialize_for_petta("\n".join(self._buffer))

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".metta", delete=False
        ) as tmp:
            tmp.write(full_code)
            tmp_path: str = tmp.name

        try:
            result = subprocess.run(
                ["sh", "run.sh", tmp_path],
                cwd=self._petta_path,
                capture_output=True,
                text=True,
                timeout=30,
            )
        finally:
            os.unlink(tmp_path)

        if result.returncode != 0:
            logger.warning(
                "PeTTa execution failed",
                returncode=result.returncode,
                stderr=result.stderr.strip(),
            )
            return []

        return _parse_petta_output(result.stdout)


class JanusPeTTaRunner:
    """In-process PeTTa runner via janus-swi — no subprocess, persistent state.

    Consults the PeTTa Prolog source once (globally), then each ``run``
    call transpiles MeTTa to Prolog and evaluates in-process.  The SWI-Prolog
    state persists across calls, so loaded knowledge accumulates.
    """

    def __init__(self, petta_path: str | None = None) -> None:
        resolved = petta_path or _default_petta_path()
        self._petta_path: str = resolved
        python_dir = os.path.join(resolved, "python")
        if python_dir not in sys.path:
            sys.path.insert(0, python_dir)

        from petta import PeTTa  # type: ignore[import-untyped]

        self._petta = PeTTa(petta_path=resolved)

    def run(self, code: str) -> Sequence[Sequence[str]]:
        serialized = _serialize_for_petta(code)
        raw = self._petta.process_metta_string(serialized)
        return _parse_janus_output(raw)

    def load_file(self, path: str) -> Sequence[Sequence[str]]:
        """Load a .metta file directly (avoids string-quoting issues)."""
        raw = self._petta.load_metta_file(path)
        return _parse_janus_output(raw)


def _parse_janus_output(raw: object) -> Sequence[Sequence[str]]:
    """Convert janus PeTTa results to the standard list-of-lists format."""
    if not raw:
        return []
    items: Sequence[object] = raw if isinstance(raw, (list, tuple)) else [raw]
    results: list[Sequence[str]] = []
    group: list[str] = []
    for item in items:
        text = _deserialize_from_petta(str(item))
        group.append(text)
    if group:
        results.append(group)
    return results


def _parse_petta_output(stdout: str) -> Sequence[Sequence[str]]:
    """Parse PeTTa stdout into list-of-lists using hyperon's parse_all.

    Deserializes Prolog-safe symbol names back to MeTTa operators first.
    """
    from metta_nl_corpus.lib.helpers import parse_all

    deserialized: str = _deserialize_from_petta(stdout)
    lines: list[str] = [
        ln.strip() for ln in deserialized.strip().splitlines() if ln.strip()
    ]
    results: list[Sequence[str]] = []
    for line in lines:
        if line.startswith("[") and line.endswith("]"):
            inner: str = line[1:-1].strip()
            if not inner:
                results.append([])
                continue
            atoms = parse_all(inner)
            results.append([str(a) for a in atoms])
        else:
            atoms = parse_all(line)
            results.append([str(a) for a in atoms])
    return results


def create_runner(backend: MeTTaBackend | None = None) -> MeTTaRunner:
    """Factory for MeTTa runners, defaulting to METTA_BACKEND env var."""
    if backend is None:
        raw = os.environ.get("METTA_BACKEND", MeTTaBackend.HYPERON)
        backend = MeTTaBackend(raw)

    if backend == MeTTaBackend.JANUS:
        petta_path = _default_petta_path()
        logger.info("Using JanusPeTTa backend (in-process)", petta_path=petta_path)
        return JanusPeTTaRunner(petta_path)

    if backend == MeTTaBackend.PETTA:
        petta_path = _default_petta_path()
        logger.info("Using PeTTa subprocess backend", petta_path=petta_path)
        return PeTTaRunner(petta_path)

    logger.debug("Using Hyperon backend")
    return HyperonRunner()
