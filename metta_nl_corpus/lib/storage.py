"""SQLite-backed annotation storage with WAL mode for concurrent access.

Replaces the parquet read-modify-write pattern that corrupts data under
concurrent asyncio.gather writes. SQLite in WAL mode provides ACID
guarantees with single-writer/multi-reader concurrency.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any

import polars as pl
from pandera.polars import DataFrameModel
from structlog import get_logger

from metta_nl_corpus.models import Annotation, Validation

logger = get_logger(__name__)

# Maps between the Polars/Pandera column name 'index' and the SQLite column 'idx'.
_PL_TO_SQL = {"index": "idx"}
_SQL_TO_PL = {"idx": "index"}

# Primary key column for each table.
_PRIMARY_KEYS = {"annotations": "annotation_id", "validations": "validation_id"}

# Polars dtype string -> SQLite type.
_DTYPE_MAP: dict[str, str] = {
    "String": "TEXT",
    "Utf8": "TEXT",
    "UInt32": "INTEGER",
    "Int64": "INTEGER",
    "Boolean": "INTEGER",  # SQLite has no native bool
}


def _columns_from_model(
    model: type[DataFrameModel],
    pk: str,
) -> list[tuple[str, str]]:
    """Derive SQLite column definitions from a Pandera DataFrameModel."""
    schema = model.to_schema()
    columns: list[tuple[str, str]] = []
    for name, field in schema.columns.items():
        sql_name = _PL_TO_SQL.get(name, name)
        sql_type = _DTYPE_MAP.get(str(field.dtype), "TEXT")
        constraints = " PRIMARY KEY" if name == pk else ""
        if not field.nullable and field.required and not constraints:
            constraints = " NOT NULL"
        columns.append((sql_name, f"{sql_type}{constraints}"))
    return columns


_ANNOTATIONS_COLUMNS = _columns_from_model(Annotation, _PRIMARY_KEYS["annotations"])
_VALIDATIONS_COLUMNS = _columns_from_model(Validation, _PRIMARY_KEYS["validations"])


class AnnotationStore:
    """Thread-safe SQLite store for annotations and validations."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        # Eagerly create tables on the main thread's connection.
        conn = self._get_conn()
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        self._create_tables(conn)

    # -- connection management -------------------------------------------------

    def _get_conn(self) -> sqlite3.Connection:
        """Return a per-thread connection (created lazily)."""
        conn: sqlite3.Connection | None = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return conn

    # -- schema ----------------------------------------------------------------

    def _create_tables(self, conn: sqlite3.Connection) -> None:
        cols_a = ", ".join(f"{name} {typ}" for name, typ in _ANNOTATIONS_COLUMNS)
        conn.execute(f"CREATE TABLE IF NOT EXISTS annotations ({cols_a})")

        cols_v = ", ".join(f"{name} {typ}" for name, typ in _VALIDATIONS_COLUMNS)
        conn.execute(f"CREATE TABLE IF NOT EXISTS validations ({cols_v})")
        conn.commit()
        self._migrate_columns(conn)

    def _migrate_columns(self, conn: sqlite3.Connection) -> None:
        """Add any missing columns to existing tables."""
        cur = conn.execute("PRAGMA table_info(annotations)")
        existing = {row[1] for row in cur.fetchall()}
        for col_name, col_def in _ANNOTATIONS_COLUMNS:
            if col_name not in existing:
                # Strip NOT NULL for migration safety — existing rows will be NULL.
                safe_def = col_def.replace(" NOT NULL", "")
                conn.execute(
                    f"ALTER TABLE annotations ADD COLUMN {col_name} {safe_def}"
                )
                logger.info("Migrated column", table="annotations", column=col_name)
        conn.commit()

    # -- annotations -----------------------------------------------------------

    def insert_annotation(self, row: dict[str, Any]) -> str:
        """INSERT a single annotation row. Returns the annotation_id."""
        conn = self._get_conn()
        mapped = {_PL_TO_SQL.get(k, k): v for k, v in row.items()}
        # Only keep columns that exist in the table.
        col_names = [c[0] for c in _ANNOTATIONS_COLUMNS]
        filtered = {k: v for k, v in mapped.items() if k in col_names}
        # Convert bools for SQLite.
        if "is_valid" in filtered and isinstance(filtered["is_valid"], bool):
            filtered["is_valid"] = int(filtered["is_valid"])

        cols = ", ".join(filtered.keys())
        placeholders = ", ".join("?" for _ in filtered)
        conn.execute(
            f"INSERT OR REPLACE INTO annotations ({cols}) VALUES ({placeholders})",
            list(filtered.values()),
        )
        conn.commit()
        logger.debug("Inserted annotation", annotation_id=filtered.get("annotation_id"))
        return str(filtered.get("annotation_id", ""))

    def update_annotation(self, annotation_id: str, updates: dict[str, Any]) -> None:
        """UPDATE specific columns of an annotation by annotation_id."""
        conn = self._get_conn()
        mapped = {_PL_TO_SQL.get(k, k): v for k, v in updates.items()}
        if "is_valid" in mapped and isinstance(mapped["is_valid"], bool):
            mapped["is_valid"] = int(mapped["is_valid"])
        set_clause = ", ".join(f"{k} = ?" for k in mapped)
        conn.execute(
            f"UPDATE annotations SET {set_clause} WHERE annotation_id = ?",
            [*mapped.values(), annotation_id],
        )
        conn.commit()

    def get_annotation(self, annotation_id: str) -> dict[str, Any] | None:
        """Fetch a single annotation by id, or None."""
        conn = self._get_conn()
        cur = conn.execute(
            "SELECT * FROM annotations WHERE annotation_id = ?", (annotation_id,)
        )
        row = cur.fetchone()
        if row is None:
            return None
        return self._row_to_dict(dict(row), source="annotations")

    def query(
        self,
        table: str = "annotations",
        filter_column: str | None = None,
        filter_value: str | list[str] | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        """SELECT with optional filter. Returns {total, returned, columns, rows}."""
        conn = self._get_conn()
        valid_tables = {"annotations", "validations"}
        if table not in valid_tables:
            return {"error": f"Unknown table '{table}'. Use: {', '.join(valid_tables)}"}

        # Map Polars column names to SQL column names for filtering.
        sql_filter_col = (
            _PL_TO_SQL.get(filter_column, filter_column) if filter_column else None
        )

        # Build WHERE clause depending on scalar vs list filter_value.
        where, params = self._build_where(sql_filter_col, filter_value)

        # Total count
        cur = conn.execute(f"SELECT COUNT(*) FROM {table}{where}", params)
        total = cur.fetchone()[0]

        # Fetch rows
        cur = conn.execute(f"SELECT * FROM {table}{where} LIMIT ?", (*params, limit))

        rows = [self._row_to_dict(dict(r), source=table) for r in cur.fetchall()]
        col_names = (
            [_SQL_TO_PL.get(c[0], c[0]) for c in _ANNOTATIONS_COLUMNS]
            if table == "annotations"
            else [c[0] for c in _VALIDATIONS_COLUMNS]
        )

        return {
            "total": total,
            "returned": len(rows),
            "columns": col_names,
            "rows": rows,
        }

    # -- validations -----------------------------------------------------------

    def insert_validation(self, row: dict[str, Any]) -> str:
        """INSERT a single validation row. Returns the validation_id."""
        conn = self._get_conn()
        col_names = [c[0] for c in _VALIDATIONS_COLUMNS]
        filtered = {k: v for k, v in row.items() if k in col_names}
        if "is_valid" in filtered and isinstance(filtered["is_valid"], bool):
            filtered["is_valid"] = int(filtered["is_valid"])

        cols = ", ".join(filtered.keys())
        placeholders = ", ".join("?" for _ in filtered)
        conn.execute(
            f"INSERT OR REPLACE INTO validations ({cols}) VALUES ({placeholders})",
            list(filtered.values()),
        )
        conn.commit()
        return str(filtered.get("validation_id", ""))

    # -- export / import -------------------------------------------------------

    def to_polars(self, table: str = "annotations") -> pl.DataFrame:
        """Export full table as a Polars DataFrame."""
        conn = self._get_conn()
        cur = conn.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
        if not rows:
            return pl.DataFrame()
        dicts = [self._row_to_dict(dict(r), source=table) for r in rows]
        # Use a generous infer_schema_length to handle mixed types
        return pl.DataFrame(dicts, infer_schema_length=len(dicts))

    def export_parquet(self, path: Path, table: str = "annotations") -> int:
        """Export table to parquet. Returns row count."""
        df = self.to_polars(table)
        if df.is_empty():
            logger.info("Nothing to export", table=table)
            return 0
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path)
        logger.info("Exported to parquet", path=str(path), rows=len(df))
        return len(df)

    def import_parquet(self, path: Path, table: str = "annotations") -> int:
        """Import rows from a parquet file. Returns number of rows imported."""
        if not path.exists():
            logger.warning("Parquet file not found", path=str(path))
            return 0
        df = pl.read_parquet(path)
        count = 0
        for row in df.to_dicts():
            if table == "annotations":
                self.insert_annotation(row)
            else:
                self.insert_validation(row)
            count += 1
        logger.info("Imported from parquet", path=str(path), rows=count, table=table)
        return count

    # -- helpers ---------------------------------------------------------------

    @staticmethod
    def _build_where(
        column: str | None, value: str | list[str] | None
    ) -> tuple[str, tuple[str, ...]]:
        """Return a (WHERE clause, params) tuple.

        Accepts a single string or a list of strings. Strings are never
        iterated character-by-character — only ``list`` triggers IN().
        """
        if not column or value is None:
            return "", ()
        if isinstance(value, list):
            if not value:
                return " WHERE 1=0", ()
            placeholders = ", ".join("?" for _ in value)
            return f" WHERE {column} IN ({placeholders})", tuple(value)
        return f" WHERE {column} = ?", (value,)

    @staticmethod
    def _row_to_dict(
        row: dict[str, Any], source: str = "annotations"
    ) -> dict[str, Any]:
        """Convert a SQLite row dict back to Polars-compatible column names and types."""
        if source == "annotations":
            # Rename idx -> index
            if "idx" in row:
                row["index"] = row.pop("idx")
            # Convert is_valid back to bool
            if "is_valid" in row and row["is_valid"] is not None:
                row["is_valid"] = bool(row["is_valid"])
        elif source == "validations":
            if "is_valid" in row and row["is_valid"] is not None:
                row["is_valid"] = bool(row["is_valid"])
        return row
