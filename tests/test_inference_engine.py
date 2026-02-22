from pathlib import Path

from metta_nl_corpus.services.defs.transformation.assets import (
    validate_expressions_are_contradictory,
    validate_expressions_are_entailing,
)


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
