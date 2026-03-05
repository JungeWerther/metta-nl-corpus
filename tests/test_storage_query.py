"""Tests for AnnotationStore.query with scalar and list filter_value."""

from __future__ import annotations


import pytest

from metta_nl_corpus.lib.storage import AnnotationStore


@pytest.fixture()
def store(tmp_path):
    """Create a fresh in-memory-like store with three annotations."""
    s = AnnotationStore(db_path=tmp_path / "test.db")
    for i, label in enumerate(["entailment", "contradiction", "neutral"]):
        s.insert_annotation(
            {
                "annotation_id": f"id-{i}",
                "idx": i,
                "label": label,
                "premise": f"premise {i}",
                "hypothesis": f"hypothesis {i}",
                "metta_premise": f"(premise-{i})",
                "metta_hypothesis": f"(hypothesis-{i})",
                "model": "test",
                "generation_model": "test",
                "system_prompt": "test prompt",
                "is_valid": True,
                "version": "0.0.1",
            }
        )
    return s


class TestScalarFilterValue:
    def test_single_string_returns_one_row(self, store):
        result = store.query(filter_column="label", filter_value="entailment")
        assert result["total"] == 1
        assert result["rows"][0]["label"] == "entailment"

    def test_string_not_iterated_as_chars(self, store):
        """A string like 'neutral' must NOT be split into ['n','e','u',...]."""
        result = store.query(filter_column="label", filter_value="neutral")
        assert result["total"] == 1
        assert result["rows"][0]["label"] == "neutral"

    def test_no_filter_returns_all(self, store):
        result = store.query()
        assert result["total"] == 3


class TestListFilterValue:
    def test_list_of_ids(self, store):
        result = store.query(
            filter_column="annotation_id", filter_value=["id-0", "id-2"]
        )
        assert result["total"] == 2
        returned_ids = {r["annotation_id"] for r in result["rows"]}
        assert returned_ids == {"id-0", "id-2"}

    def test_list_with_single_element(self, store):
        result = store.query(filter_column="label", filter_value=["contradiction"])
        assert result["total"] == 1

    def test_empty_list_returns_nothing(self, store):
        result = store.query(filter_column="label", filter_value=[])
        assert result["total"] == 0
        assert result["rows"] == []

    def test_list_with_nonexistent_values(self, store):
        result = store.query(filter_column="label", filter_value=["foo", "bar"])
        assert result["total"] == 0


class TestBuildWhereUnit:
    """Direct unit tests for _build_where to verify SQL generation."""

    def test_none_value(self):
        clause, params = AnnotationStore._build_where("col", None)
        assert clause == ""
        assert params == ()

    def test_none_column(self):
        clause, params = AnnotationStore._build_where(None, "val")
        assert clause == ""
        assert params == ()

    def test_scalar(self):
        clause, params = AnnotationStore._build_where("col", "val")
        assert clause == " WHERE col = ?"
        assert params == ("val",)

    def test_list(self):
        clause, params = AnnotationStore._build_where("col", ["a", "b", "c"])
        assert clause == " WHERE col IN (?, ?, ?)"
        assert params == ("a", "b", "c")

    def test_empty_list(self):
        clause, params = AnnotationStore._build_where("col", [])
        assert clause == " WHERE 1=0"
        assert params == ()

    def test_string_is_not_list(self):
        """Critically: a string must produce = ?, not IN (?, ?, ...)."""
        clause, params = AnnotationStore._build_where("col", "abc")
        assert "IN" not in clause
        assert params == ("abc",)
