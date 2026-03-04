"""Tests for the MeTTa runner adapter, focusing on PeTTa symbol serde."""

import pytest

from metta_nl_corpus.lib.runner import (
    _deserialize_from_petta,
    _serialize_for_petta,
)


@pytest.mark.parametrize(
    ("metta_code", "expected_fragment"),
    [
        ("(=> A B)", "(__metta_implies__ A B)"),
        ("(> x 5)", "(__metta_gt__ x 5)"),
        ("(< x 5)", "(__metta_lt__ x 5)"),
        ("(>= x 5)", "(__metta_gte__ x 5)"),
        ("(<= x 5)", "(__metta_lte__ x 5)"),
        (
            "(≞ (=> A B) (STV 1.0 1.0))",
            "(__metta_approx__ (__metta_implies__ A B) (STV 1.0 1.0))",
        ),
        ("⊥", "__metta_bottom__"),
    ],
)
def test_serialize_for_petta(metta_code: str, expected_fragment: str) -> None:
    assert _serialize_for_petta(metta_code) == expected_fragment


@pytest.mark.parametrize(
    ("prolog_text", "expected_metta"),
    [
        ("(__metta_implies__ A B)", "(=> A B)"),
        ("(__metta_gt__ x 5)", "(> x 5)"),
        ("(__metta_bottom__)", "(⊥)"),
        ("(__metta_gte__ x 5)", "(>= x 5)"),
        ("(__metta_lte__ x 5)", "(<= x 5)"),
    ],
)
def test_deserialize_from_petta(prolog_text: str, expected_metta: str) -> None:
    assert _deserialize_from_petta(prolog_text) == expected_metta


def test_serde_roundtrip() -> None:
    original = "(=> (, (> $x $a) (< $x $b)) ⊥)"
    assert _deserialize_from_petta(_serialize_for_petta(original)) == original


def test_serde_multiline_roundtrip() -> None:
    original = """(= (get-tv (=> (, (> $x $a) (<= $x $b)) ⊥))
   (if (<= $b $a) (STV 1.0 1.0) (STV 0.0 0.0)))"""
    assert _deserialize_from_petta(_serialize_for_petta(original)) == original
