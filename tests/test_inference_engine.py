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
    assert "0.97" in str(result[-1][0])
    assert "0.95" in str(result[-1][0])


def test_boolean_inference_with_tv_propositions():
    """Transitivity still works on TV-annotated facts."""
    result = _run_with_inference(
        "!(add-proposition-tv (shedsFur Dog) (STV 0.97 0.95))",
        "!(add-proposition-tv (is-a a-dog Dog) (STV 1.0 0.99))",
        "!(find-evidence-for (shedsFur a-dog))",
    )
    assert result and len(result[-1]) > 0


def test_combine_tv():
    result = _run_with_inference(
        "!(combine-tv (STV 0.9 0.8) (STV 0.7 0.6))",
    )
    assert result and len(result[-1]) > 0
    tv_str = str(result[-1][0])
    # s = 0.9 * 0.7 = 0.63, c = min(0.8, 0.6) = 0.6
    assert "0.63" in tv_str or "0.6300" in tv_str
    assert "0.6" in tv_str


def test_find_evidence_for_tv():
    result = _run_with_inference(
        "!(add-proposition-tv (shedsFur Dog) (STV 0.97 0.95))",
        "!(find-evidence-for-tv (shedsFur Dog))",
    )
    assert result and len(result[-1]) > 0
    tv_str = str(result[-1][0])
    assert "≞" in tv_str
    assert "0.97" in tv_str


def test_contrary_attribute_contradiction():
    """contraryAttribute triggers ⊥."""
    result = _run_with_inference(
        "!(add-contrary priceAbove60k priceBelow50k)",
        "!(add-proposition (priceAbove60k BTC))",
        "!(add-proposition (priceBelow50k BTC))",
        "!(find-evidence-for ⊥)",
    )
    assert result and len(result[-1]) > 0


def test_contrary_attribute_symmetric():
    """Contradiction works in reverse order too."""
    result = _run_with_inference(
        "!(add-contrary priceAbove60k priceBelow50k)",
        "!(add-proposition (priceBelow50k BTC))",
        "!(add-proposition (priceAbove60k BTC))",
        "!(find-evidence-for ⊥)",
    )
    assert result and len(result[-1]) > 0


def test_prediction_contradiction():
    """Full scenario: 'BTC above 60k' vs 'BTC below 50k' with TVs."""
    result = _run_with_inference(
        "!(add-contrary priceAbove60k priceBelow50k)",
        "!(add-proposition-tv (priceAbove60k BTC) (STV 0.7 0.6))",
        "!(add-proposition-tv (priceBelow50k BTC) (STV 0.4 0.3))",
        "!(find-evidence-for ⊥)",
    )
    assert result and len(result[-1]) > 0


def test_prediction_no_contradiction():
    """Overlapping ranges don't contradict without contraryAttribute."""
    result = _run_with_inference(
        "!(add-proposition-tv (priceAbove60k BTC) (STV 0.7 0.6))",
        "!(add-proposition-tv (priceBelow70k BTC) (STV 0.8 0.5))",
        "!(find-evidence-for ⊥)",
    )
    # No contraryAttribute declared, so no contradiction
    assert not result or len(result[-1]) == 0


def test_prediction_entailment_with_tv():
    """'BTC above 60k' entails 'BTC above 50k' via transitivity."""
    result = _run_with_inference(
        "!(add-proposition-tv (priceAbove60k BTC) (STV 0.7 0.6))",
        "!(add-proposition (priceAbove50k priceAbove60k))",
        "!(find-evidence-for (priceAbove50k BTC))",
    )
    assert result and len(result[-1]) > 0
