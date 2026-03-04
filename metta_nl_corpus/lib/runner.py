"""MeTTa execution backend adapter.

Provides a unified interface for running MeTTa code via either the
hyperon-experimental runtime or the PeTTa (Prolog-based) transpiler.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from collections.abc import Sequence
from enum import StrEnum
from typing import Protocol

from structlog import get_logger

logger = get_logger(__name__)


class MeTTaBackend(StrEnum):
    HYPERON = "hyperon"
    PETTA = "petta"


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
        full_code: str = "\n".join(self._buffer)

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


def _parse_petta_output(stdout: str) -> Sequence[Sequence[str]]:
    """Parse PeTTa stdout into list-of-lists using hyperon's parse_all."""
    from metta_nl_corpus.lib.helpers import parse_all

    lines: list[str] = [ln.strip() for ln in stdout.strip().splitlines() if ln.strip()]
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
        backend = MeTTaBackend(os.environ.get("METTA_BACKEND", MeTTaBackend.HYPERON))

    if backend == MeTTaBackend.PETTA:
        petta_path: str | None = os.environ.get("PETTA_PATH")
        if not petta_path:
            msg = "PETTA_PATH env var is required when METTA_BACKEND=petta"
            raise ValueError(msg)
        logger.info("Using PeTTa backend", petta_path=petta_path)
        return PeTTaRunner(petta_path)

    logger.debug("Using Hyperon backend")
    return HyperonRunner()
