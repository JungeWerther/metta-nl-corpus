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


def _make_row(annotation_id: str, premise: str, hypothesis: str, **overrides):
    """Helper to build a minimal annotation row."""
    row = {
        "annotation_id": annotation_id,
        "idx": 0,
        "label": "entailment",
        "premise": premise,
        "hypothesis": hypothesis,
        "metta_premise": f"({premise})",
        "metta_hypothesis": f"({hypothesis})",
        "generation_model": "test",
        "system_prompt": "test",
        "is_valid": True,
        "version": "0.0.1",
    }
    row.update(overrides)
    return row


class TestDeduplicate:
    def test_removes_duplicate_premise_hypothesis_pairs(self, tmp_path):
        s = AnnotationStore(db_path=tmp_path / "dedup.db")
        s.insert_annotation(_make_row("id-1", "A dog runs", "An animal moves"))
        s.insert_annotation(_make_row("id-2", "A dog runs", "An animal moves"))
        s.insert_annotation(_make_row("id-3", "A dog runs", "An animal moves"))
        s.insert_annotation(_make_row("id-4", "A cat sits", "A feline rests"))

        deleted = s.deduplicate()

        assert deleted == 2
        result = s.query()
        assert result["total"] == 2
        # The latest rowid (id-3) should be kept for the duplicated pair
        ids = {r["annotation_id"] for r in result["rows"]}
        assert "id-3" in ids
        assert "id-4" in ids

    def test_no_duplicates_deletes_nothing(self, tmp_path):
        s = AnnotationStore(db_path=tmp_path / "nodedup.db")
        s.insert_annotation(_make_row("id-1", "A", "B"))
        s.insert_annotation(_make_row("id-2", "C", "D"))

        assert s.deduplicate() == 0
        assert s.query()["total"] == 2


class TestUpsertAnnotation:
    def test_inserts_when_new(self, tmp_path):
        s = AnnotationStore(db_path=tmp_path / "upsert.db")
        aid = s.upsert_annotation(_make_row("id-new", "P", "H"))

        assert aid == "id-new"
        assert s.query()["total"] == 1

    def test_updates_existing_without_duplicating(self, tmp_path):
        s = AnnotationStore(db_path=tmp_path / "upsert.db")
        s.insert_annotation(_make_row("id-1", "P", "H"))

        s.upsert_annotation({"annotation_id": "id-1", "metta_premise": "(updated)"})

        assert s.query()["total"] == 1
        row = s.get_annotation("id-1")
        assert row is not None
        assert row["metta_premise"] == "(updated)"
        # Original fields preserved
        assert row["premise"] == "P"
        assert row["hypothesis"] == "H"
