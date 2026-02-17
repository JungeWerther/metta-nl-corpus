from metta_nl_corpus.services.defs.transformation.assets import (
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
