from pathlib import Path

from hyperon import MeTTa

from metta_nl_corpus.services.defs.transformation.assets import (
    validate_expressions_are_contradictory,
    validate_expressions_are_entailing,
)

INFERENCE_PATH = (
    Path(__file__).parent.parent
    / "metta_nl_corpus"
    / "services"
    / "spaces"
    / "inference.metta"
)


def _run_with_inference(*expressions: str) -> list:
    """Load inference.metta, run expressions, return last result."""
    runner = MeTTa()
    runner.run(INFERENCE_PATH.read_text())
    result = None
    for expr in expressions:
        result = runner.run(expr)
    return result


def test_expression_entails_itself():
    assert validate_expressions_are_entailing("A", "A")
    assert validate_expressions_are_entailing("(A)", "(A)")
    assert validate_expressions_are_entailing("(A B)", "(A B)")
    assert validate_expressions_are_entailing("(A B C)", "(A B C)")


def test_transitivity_on_unary_atoms():
    assert validate_expressions_are_entailing(
        "(white swan) (swan this-swan)", "(white this-swan)"
    )
    assert validate_expressions_are_entailing(
        "(white swan) (swan this-swan) (entity this-swan) (exists entity)",
        "(white this-swan) (exists this-swan)",
    )
    assert not validate_expressions_are_entailing(
        "(white swan) (swan this-swan) (entity this-swan) (exists entity)",
        "(white this-swan) (exists swan)",
    )


def test_expression_works_on_binary_atoms():
    assert validate_expressions_are_entailing(
        "(held-by this-swan this-human)", "(held-by this-swan this-human)"
    )

    assert validate_expressions_are_entailing(
        "(held-by this-swan this-human) (human this-human)",
        "(held-by this-swan human)",
        verbose=True,
    )

    assert validate_expressions_are_entailing(
        "(held-by this-swan this-human) (animal this-swan)",
        "(held-by animal this-human)",
        verbose=True,
    )


def test_basic_contradictions():
    assert validate_expressions_are_contradictory("(A B)", "(is-not A B)")
    assert validate_expressions_are_contradictory("(A B)", "(is-not (A B))")
    assert validate_expressions_are_contradictory("(A B)", "((is-not A) B)")
    assert not validate_expressions_are_contradictory("(A B)", "((is-not B) A)")


def test_no_contradiction():
    data = (
        Path(__file__).parent.parent
        / "metta_nl_corpus"
        / "services"
        / "spaces"
        / "non-contradiction.metta"
    ).read_text()

    assert not validate_expressions_are_contradictory("", data, verbose=True)


# === PLN Truth Value Tests ===


def test_add_proposition_tv_and_get_tv():
    result = _run_with_inference(
        "!(add-proposition-tv (shedsFur Dog) (STV 0.97 0.95))",
        "!(get-tv (shedsFur Dog))",
    )
    assert result and len(result[-1]) > 0
    tv_str = str(result[-1][0])
    assert "0.97" in tv_str
    assert "0.95" in tv_str


def test_combine_tv():
    result = _run_with_inference(
        "!(combine-tv (STV 0.9 0.8) (STV 0.7 0.6))",
    )
    tv_str = str(result[-1][0])
    # s = 0.9 * 0.7 = 0.63, c = min(0.8, 0.6) = 0.6
    assert "0.63" in tv_str or "0.6300" in tv_str


# === Numeric Contradiction Tests ===


def _assert_contradiction_tv(result, expected_nonzero=True):
    """Check that find-evidence-for-tv ⊥ returns meaningful STV."""
    assert result and len(result[-1]) > 0
    tv_str = str(result[-1][0])
    assert "≞" in tv_str
    if expected_nonzero:
        assert "(STV 0.0 0.0)" not in tv_str
    else:
        assert "(STV 0.0 0.0)" in tv_str


def test_numeric_contradiction_gt_lt():
    """(> X 60) ∧ (< X 50) → ⊥ because 50 ≤ 60."""
    result = _run_with_inference(
        "!(add-atom &a (> price 60))",
        "!(add-atom &a (< price 50))",
        "!(find-evidence-for ⊥)",
    )
    assert result and len(result[-1]) > 0


def test_numeric_no_contradiction_overlapping():
    """(> X 60) ∧ (< X 70) — no contradiction because ranges overlap."""
    result = _run_with_inference(
        "!(add-atom &a (> price 60))",
        "!(add-atom &a (< price 70))",
        "!(find-evidence-for ⊥)",
    )
    assert result and len(result[-1]) == 0


def test_numeric_contradiction_equal_bounds():
    """(> X 60) ∧ (< X 60) → ⊥ because 60 ≤ 60."""
    result = _run_with_inference(
        "!(add-atom &a (> price 60))",
        "!(add-atom &a (< price 60))",
        "!(find-evidence-for ⊥)",
    )
    assert result and len(result[-1]) > 0


def test_prediction_contradiction_with_tv():
    """BTC above 60k vs below 50k — contradiction with combined TVs."""
    result = _run_with_inference(
        "!(add-proposition-tv (> btc-price 60000) (STV 0.7 0.6))",
        "!(add-proposition-tv (< btc-price 50000) (STV 0.3 0.4))",
        "!(find-evidence-for-tv ⊥)",
    )
    _assert_contradiction_tv(result, expected_nonzero=True)


def test_prediction_no_contradiction_with_tv():
    """BTC above 60k and below 70k — TV is (STV 0.0 0.0) because ranges overlap."""
    result = _run_with_inference(
        "!(add-proposition-tv (> btc-price 60000) (STV 0.7 0.6))",
        "!(add-proposition-tv (< btc-price 70000) (STV 0.8 0.5))",
        "!(find-evidence-for-tv ⊥)",
    )
    _assert_contradiction_tv(result, expected_nonzero=False)


# === find-evidence-for-tv Tests ===


def test_find_evidence_for_tv_direct():
    """Direct TV lookup via find-evidence-for-tv."""
    result = _run_with_inference(
        "!(add-proposition-tv (shedsFur Dog) (STV 0.97 0.95))",
        "!(find-evidence-for-tv (shedsFur Dog))",
    )
    assert result and len(result[-1]) > 0
    tv_str = str(result[-1][0])
    assert "≞" in tv_str
    assert "0.97" in tv_str


def test_find_evidence_for_tv_transitive():
    """Transitive inference propagates TV through the chain."""
    result = _run_with_inference(
        "!(add-proposition-tv (shedsFur Dog) (STV 0.97 0.95))",
        "!(add-proposition-tv (Dog a-dog) (STV 1.0 0.99))",
        "!(find-evidence-for-tv (shedsFur a-dog))",
    )
    assert result and len(result[-1]) > 0
    tv_str = str(result[-1][0])
    assert "≞" in tv_str
    assert "0.99" in tv_str


def test_find_evidence_for_tv_bare_propositions():
    """Bare propositions (no TV) get default (STV 1.0 1.0) — identity element."""
    result = _run_with_inference(
        "!(add-proposition (white swan))",
        "!(add-proposition (swan this-swan))",
        "!(find-evidence-for-tv (white this-swan))",
    )
    assert result and len(result[-1]) > 0
    assert "(STV 1.0 1.0)" in str(result[-1][0])


def test_find_evidence_for_tv_numeric_contradiction():
    """Numeric contradiction returns (≞ ⊥ (STV s c)) with combined TVs."""
    result = _run_with_inference(
        "!(add-proposition-tv (> btc 60000) (STV 0.7 0.6))",
        "!(add-proposition-tv (< btc 50000) (STV 0.3 0.4))",
        "!(find-evidence-for-tv ⊥)",
    )
    assert result and len(result[-1]) > 0
    all_results = str(result[-1])
    assert "≞" in all_results
    assert "0.21" in all_results or "0.2100" in all_results


def test_find_evidence_for_tv_no_numeric_contradiction():
    """Overlapping ranges: rule TV is (STV 0.0 0.0), zeroes out the result."""
    result = _run_with_inference(
        "!(add-proposition-tv (> btc 60000) (STV 0.7 0.6))",
        "!(add-proposition-tv (< btc 70000) (STV 0.8 0.5))",
        "!(find-evidence-for-tv ⊥)",
    )
    assert result and len(result[-1]) > 0
    tv_str = str(result[-1][0])
    assert "(STV 0.0 0.0)" in tv_str


# === Numeric contradiction with <= and >= ===


def test_gt_lte_contradiction():
    """(> X 6) ∧ (<= X 5) → ⊥ because 5 ≤ 6."""
    result = _run_with_inference(
        "!(add-atom &a (> price 6))",
        "!(add-atom &a (<= price 5))",
        "!(find-evidence-for ⊥)",
    )
    assert result and len(result[-1]) > 0


def test_gte_lt_contradiction():
    """(>= X 6) ∧ (< X 5) → ⊥ because 5 ≤ 6."""
    result = _run_with_inference(
        "!(add-atom &a (>= price 6))",
        "!(add-atom &a (< price 5))",
        "!(find-evidence-for ⊥)",
    )
    assert result and len(result[-1]) > 0


def test_gte_lte_contradiction():
    """(>= X 6) ∧ (<= X 4) → ⊥ because 4 < 6."""
    result = _run_with_inference(
        "!(add-atom &a (>= price 6))",
        "!(add-atom &a (<= price 4))",
        "!(find-evidence-for ⊥)",
    )
    assert result and len(result[-1]) > 0


def test_gte_lte_no_contradiction_at_equal():
    """(>= X 6) ∧ (<= X 6) — NOT contradiction, X=6 works."""
    result = _run_with_inference(
        "!(add-atom &a (>= price 6))",
        "!(add-atom &a (<= price 6))",
        "!(find-evidence-for ⊥)",
    )
    assert result and len(result[-1]) == 0


def test_lte_gt_cross_bound_contradiction():
    """(<= X 5) ∧ (> X 6) → ⊥ because 5 ≤ 6."""
    result = _run_with_inference(
        "!(add-atom &a (<= price 5))",
        "!(add-atom &a (> price 6))",
        "!(find-evidence-for ⊥)",
    )
    assert result and len(result[-1]) > 0


# === Complement TV Tests ===


def test_complement_tv_lt_to_gte():
    """(< X 70000) (STV 0.7 0.8) implies (>= X 70000) (STV 0.3 0.8)."""
    result = _run_with_inference(
        "!(add-atom &a (< btc 70000))",
        "!(add-atom &a (≞ (< btc 70000) (STV 0.7 0.8)))",
        "!(get-tv (>= btc 70000))",
    )
    assert result and len(result[-1]) > 0
    tv_str = str(result[-1][0])
    assert "0.3" in tv_str
    assert "0.8" in tv_str


def test_complement_tv_gt_to_lte():
    """(> X 60000) (STV 0.8 0.6) implies (<= X 60000) (STV 0.2 0.6)."""
    result = _run_with_inference(
        "!(add-atom &a (> btc 60000))",
        "!(add-atom &a (≞ (> btc 60000) (STV 0.8 0.6)))",
        "!(get-tv (<= btc 60000))",
    )
    assert result and len(result[-1]) > 0
    tv_str = str(result[-1][0])
    assert "0.2" in tv_str or "0.19" in tv_str
    assert "0.6" in tv_str


# === Disjunction Tests ===


def test_disjunction_left_holds():
    """(or A B) succeeds when A holds."""
    result = _run_with_inference(
        "!(add-proposition (white swan))",
        "!(find-evidence-for (or (white swan) (black swan)))",
    )
    assert result and len(result[-1]) > 0


def test_disjunction_right_holds():
    """(or A B) succeeds when B holds."""
    result = _run_with_inference(
        "!(add-proposition (black swan))",
        "!(find-evidence-for (or (white swan) (black swan)))",
    )
    assert result and len(result[-1]) > 0


def test_disjunction_neither_holds():
    """(or A B) returns empty when neither holds."""
    result = _run_with_inference(
        "!(find-evidence-for (or (white swan) (black swan)))",
    )
    assert result and len(result[-1]) == 0


def test_disjunction_tv_left():
    """(or A B) with TV returns TV of whichever branch holds."""
    result = _run_with_inference(
        "!(add-proposition-tv (white swan) (STV 0.8 0.9))",
        "!(find-evidence-for-tv (or (white swan) (black swan)))",
    )
    assert result and len(result[-1]) > 0
    tv_str = str(result[-1][0])
    assert "0.8" in tv_str
    assert "0.9" in tv_str


def test_disjunction_tv_right():
    """(or A B) with TV returns TV of whichever branch holds."""
    result = _run_with_inference(
        "!(add-proposition-tv (black swan) (STV 0.6 0.7))",
        "!(find-evidence-for-tv (or (white swan) (black swan)))",
    )
    assert result and len(result[-1]) > 0
    tv_str = str(result[-1][0])
    assert "0.6" in tv_str
    assert "0.7" in tv_str
