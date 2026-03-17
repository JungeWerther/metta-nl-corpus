"""End-to-end inference tests using the JanusPeTTa (Prolog) backend.

Mirrors key tests from test_inference_engine.py to verify that PeTTa
produces identical results to the Hyperon runner.  Uses inference-petta.metta
(the Prolog-safe variant) loaded via JanusPeTTaRunner.load_file, which is
the same path the MCP server uses.

Because JanusPeTTa shares a single in-process Prolog engine, each test
uses a fresh subprocess to guarantee isolation.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest


def _run_petta_script(script: str) -> str:
    """Run a PeTTa test script in an isolated subprocess."""
    wrapper = textwrap.dedent(f"""\
        import sys, os
        sys.path.insert(0, os.path.join(os.environ.get("PETTA_PATH", os.path.expanduser("~/sites/PeTTa")), "python"))
        from metta_nl_corpus.lib.runner import JanusPeTTaRunner
        from pathlib import Path

        INFERENCE_PETTA_PATH = str(
            Path("metta_nl_corpus/services/spaces/inference-petta.metta")
        )

        runner = JanusPeTTaRunner()
        runner.load_file(INFERENCE_PETTA_PATH)

        {textwrap.indent(script, "        ").strip()}
    """)
    result = subprocess.run(
        [sys.executable, "-c", wrapper],
        capture_output=True,
        text=True,
        timeout=30,
        cwd=str(__import__("pathlib").Path(__file__).parent.parent),
    )
    if result.returncode != 0:
        pytest.fail(f"Script failed:\n{result.stderr}")
    return result.stdout.strip()


def _truthy(output: str) -> bool:
    return output == "True"


# === Entailment ===


def test_direct_entailment():
    out = _run_petta_script("""\
runner.run("!(add-proposition (A B))")
r = runner.run("!(find-evidence-for (A B))")
print(bool(r and len(r[-1]) > 0))
""")
    assert _truthy(out)


def test_transitivity_unary():
    out = _run_petta_script("""\
runner.run("!(add-proposition (white swan))")
runner.run("!(add-proposition (swan this-swan))")
r = runner.run("!(find-evidence-for (white this-swan))")
print(bool(r and len(r[-1]) > 0))
""")
    assert _truthy(out)


def test_transitivity_two_hop():
    out = _run_petta_script("""\
runner.run("!(add-proposition (animal mammal))")
runner.run("!(add-proposition (mammal dog))")
runner.run("!(add-proposition (dog fido))")
r = runner.run("!(find-evidence-for (animal fido))")
print(bool(r and len(r[-1]) > 0))
""")
    assert _truthy(out)


def test_binary_entailment():
    out = _run_petta_script("""\
runner.run("!(add-proposition (held-by this-swan this-human))")
runner.run("!(add-proposition (human this-human))")
r = runner.run("!(find-evidence-for (held-by this-swan human))")
print(bool(r and len(r[-1]) > 0))
""")
    assert _truthy(out)


# === Contradiction ===


def test_basic_contradiction():
    out = _run_petta_script("""\
runner.run("!(add-proposition (A B))")
runner.run("!(add-proposition (is-not A B))")
r = runner.run("!(find-evidence-for ⊥)")
print(bool(r and len(r[-1]) > 0))
""")
    assert _truthy(out), "should detect (A B) vs (is-not A B) as contradiction"


def test_contradiction_wrapped_form():
    out = _run_petta_script("""\
runner.run("!(add-proposition (A B))")
runner.run("!(add-proposition (is-not (A B)))")
r = runner.run("!(find-evidence-for ⊥)")
print(bool(r and len(r[-1]) > 0))
""")
    assert _truthy(out)


def test_contradiction_curried_form():
    out = _run_petta_script("""\
runner.run("!(add-proposition (A B))")
runner.run("!(add-proposition ((is-not A) B))")
r = runner.run("!(find-evidence-for ⊥)")
print(bool(r and len(r[-1]) > 0))
""")
    assert _truthy(out)


def test_binary_predicate_contradiction():
    out = _run_petta_script("""\
runner.run("!(add-proposition (held-by swan human))")
runner.run("!(add-proposition (is-not held-by swan human))")
r = runner.run("!(find-evidence-for ⊥)")
print(bool(r and len(r[-1]) > 0))
""")
    assert _truthy(out)


def test_contradiction_via_transitivity():
    out = _run_petta_script("""\
runner.run("!(add-proposition (held-by this-swan this-human))")
runner.run("!(add-proposition (human this-human))")
runner.run("!(add-proposition (is-not held-by this-swan human))")
r = runner.run("!(find-evidence-for ⊥)")
print(bool(r and len(r[-1]) > 0))
""")
    assert _truthy(out)


def test_no_contradiction():
    out = _run_petta_script("""\
runner.run("!(add-proposition (A B))")
runner.run("!(add-proposition (C D))")
r = runner.run("!(find-evidence-for ⊥)")
print(bool(r and len(r[-1]) > 0))
""")
    assert not _truthy(out), "unrelated facts should not contradict"


# === Numeric contradiction ===


@pytest.mark.xfail(
    reason="PeTTa cannot evaluate numeric guards (if (< $b $a) ...) under Prolog instantiation"
)
def test_numeric_contradiction_gt_lt():
    out = _run_petta_script("""\
runner.run("!(add-atom &a (> price 60))")
runner.run("!(add-atom &a (< price 50))")
r = runner.run("!(find-evidence-for ⊥)")
print(bool(r and len(r[-1]) > 0))
""")
    assert _truthy(out)


def test_numeric_no_contradiction_overlapping():
    out = _run_petta_script("""\
runner.run("!(add-atom &a (> price 60))")
runner.run("!(add-atom &a (< price 70))")
r = runner.run("!(find-evidence-for ⊥)")
print(bool(r and len(r[-1]) > 0))
""")
    assert not _truthy(out)


# === Open variable queries ===


def test_open_variable_query():
    out = _run_petta_script("""\
runner.run("!(add-proposition (white swan))")
runner.run("!(add-proposition (swan fido))")
r = runner.run("!(find-evidence-for ($what fido))")
results = " ".join(str(x) for x in r[-1]) if r and r[-1] else ""
print("swan" in results and "white" in results)
""")
    assert _truthy(out)


# === Match consistency (regression for serialization mismatch) ===


def test_load_file_match_consistency():
    """Atoms stored via load_file must be matchable with structured patterns.

    Regression test for the bug where load_file stored '=>' but runtime
    queries used '__metta_implies__', making preloaded axioms invisible.
    """
    out = _run_petta_script("""\
r = runner.run("!(match &a (=> $x ⊥) $x)")
print(bool(r and len(r[-1]) > 0))
""")
    assert _truthy(out), (
        "contradiction axioms loaded via load_file should be matchable "
        "with structured (=> $x ⊥) pattern"
    )
