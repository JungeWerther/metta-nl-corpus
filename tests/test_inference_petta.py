"""End-to-end inference tests using the JanusPeTTa (Prolog) backend.

Mirrors key tests from test_inference_engine.py to verify that PeTTa
produces identical results to the Hyperon runner.  Uses inference-petta.metta
(the Prolog-safe variant) loaded via JanusPeTTaRunner.load_file, which is
the same path the MCP server uses.
"""

from __future__ import annotations

from pathlib import Path

import pytest

try:
    from metta_nl_corpus.lib.runner import JanusPeTTaRunner

    _runner = JanusPeTTaRunner()
    _HAS_PETTA = True
except (ImportError, ValueError):
    _HAS_PETTA = False
    _runner = None  # type: ignore[assignment]

pytestmark = pytest.mark.skipif(not _HAS_PETTA, reason="PeTTa/janus-swi not available")

INFERENCE_PETTA_PATH = str(
    Path(__file__).parent.parent
    / "metta_nl_corpus"
    / "services"
    / "spaces"
    / "inference-petta.metta"
)


@pytest.fixture()
def runner():
    with _runner.fresh(INFERENCE_PETTA_PATH) as r:
        yield r


def _truthy(result: list) -> bool:
    return bool(result and len(result[-1]) > 0)


# === Entailment ===


def test_direct_entailment(runner):
    runner.run("!(add-proposition (A B))")
    assert _truthy(runner.run("!(find-evidence-for (A B))"))


def test_transitivity_unary(runner):
    runner.run("!(add-proposition (white swan))")
    runner.run("!(add-proposition (swan this-swan))")
    assert _truthy(runner.run("!(find-evidence-for (white this-swan))"))


def test_transitivity_two_hop(runner):
    runner.run("!(add-proposition (animal mammal))")
    runner.run("!(add-proposition (mammal dog))")
    runner.run("!(add-proposition (dog fido))")
    assert _truthy(runner.run("!(find-evidence-for (animal fido))"))


def test_binary_entailment(runner):
    runner.run("!(add-proposition (held-by this-swan this-human))")
    runner.run("!(add-proposition (human this-human))")
    assert _truthy(runner.run("!(find-evidence-for (held-by this-swan human))"))


# === Contradiction ===


def test_basic_contradiction(runner):
    runner.run("!(add-proposition (A B))")
    runner.run("!(add-proposition (is-not A B))")
    r = runner.run("!(find-evidence-for ⊥)")
    assert _truthy(r), "should detect (A B) vs (is-not A B) as contradiction"


def test_contradiction_wrapped_form(runner):
    runner.run("!(add-proposition (A B))")
    runner.run("!(add-proposition (is-not (A B)))")
    assert _truthy(runner.run("!(find-evidence-for ⊥)"))


def test_contradiction_curried_form(runner):
    runner.run("!(add-proposition (A B))")
    runner.run("!(add-proposition ((is-not A) B))")
    assert _truthy(runner.run("!(find-evidence-for ⊥)"))


def test_binary_predicate_contradiction(runner):
    runner.run("!(add-proposition (held-by swan human))")
    runner.run("!(add-proposition (is-not held-by swan human))")
    assert _truthy(runner.run("!(find-evidence-for ⊥)"))


def test_contradiction_via_transitivity(runner):
    runner.run("!(add-proposition (held-by this-swan this-human))")
    runner.run("!(add-proposition (human this-human))")
    runner.run("!(add-proposition (is-not held-by this-swan human))")
    assert _truthy(runner.run("!(find-evidence-for ⊥)"))


def test_no_contradiction(runner):
    runner.run("!(add-proposition (A B))")
    runner.run("!(add-proposition (C D))")
    assert not _truthy(runner.run("!(find-evidence-for ⊥)"))


# === Numeric contradiction ===


@pytest.mark.xfail(
    reason="PeTTa cannot evaluate numeric guards under Prolog instantiation"
)
def test_numeric_contradiction_gt_lt(runner):
    runner.run("!(add-atom &a (> price 60))")
    runner.run("!(add-atom &a (< price 50))")
    assert _truthy(runner.run("!(find-evidence-for ⊥)"))


def test_numeric_no_contradiction_overlapping(runner):
    runner.run("!(add-atom &a (> price 60))")
    runner.run("!(add-atom &a (< price 70))")
    assert not _truthy(runner.run("!(find-evidence-for ⊥)"))


# === Open variable queries ===


def test_open_variable_query(runner):
    runner.run("!(add-proposition (white swan))")
    runner.run("!(add-proposition (swan fido))")
    r = runner.run("!(find-evidence-for ($what fido))")
    assert _truthy(r)
    all_results = " ".join(str(x) for x in r[-1])
    assert "swan" in all_results
    assert "white" in all_results


# === Match consistency (regression for serialization mismatch) ===


def test_load_file_match_consistency(runner):
    """Atoms stored via load_file must be matchable with structured patterns.

    Regression test for the bug where load_file stored '=>' but runtime
    queries used '__metta_implies__', making preloaded axioms invisible.
    """
    r = runner.run("!(match &a (=> $x ⊥) $x)")
    assert _truthy(r), (
        "contradiction axioms loaded via load_file should be matchable "
        "with structured (=> $x ⊥) pattern"
    )
