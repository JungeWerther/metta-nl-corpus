from dataclasses import dataclass
from typing import Any
import polars as pl

from dagster._core.execution.context.asset_execution_context import (
    AssetExecutionContext,
)

from metta_nl_corpus.lib.interfaces import Fn, Transformation


class Always[T]:
    @staticmethod
    def validate(value: T) -> T:
        return value


@dataclass
class Box[T]:
    data: T
    context: AssetExecutionContext | None = None

    def __or__[U](self, f: Fn["Box[T]", "Box[U]"]) -> "Box[U]":
        return f(self)


def bind(f: Fn):
    def unit(box: Box[pl.DataFrame]) -> Box[pl.DataFrame]:
        return f(box)

    return unit


def info(msg: str, *args: Any):
    def unit(box: Box[pl.DataFrame]) -> Box[pl.DataFrame]:
        assert box.context
        box.context.log.info(msg, *args)
        return box

    return unit


def with_context(context: AssetExecutionContext) -> Transformation[Box[pl.DataFrame]]:
    def unit(box: Box[pl.DataFrame]) -> Box[pl.DataFrame]:
        return Box(box.data, context)

    return unit


def on_data[T, U](f: Fn[pl.DataFrame, U]) -> Fn[Box[pl.DataFrame], Box[U]]:
    def unit(box: Box[pl.DataFrame]) -> Box[U]:
        return Box(f(box.data), box.context)

    return unit
