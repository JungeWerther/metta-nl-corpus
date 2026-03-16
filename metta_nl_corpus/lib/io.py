"""IO monad for MeTTa expression pipeline.

Vendored from JungeWerther/from (MIT license) with MeTTa-specific extensions.
Wraps side-effectful operations (parsing, validation, storage) in a monadic
chain so that each step is composable, traceable, and short-circuit-safe.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

from structlog import get_logger

logger = get_logger(__name__)

T = TypeVar("T")
U = TypeVar("U")


# ---------------------------------------------------------------------------
# Core From monad (vendored from encapsulation.base, MIT)
# ---------------------------------------------------------------------------


class From[T]:
    """Base monadic wrapper with bind (<<) and effect (&) operators."""

    def __init__(self, val: T = None) -> None:
        self.val = val

    def __eq__(self, other: object) -> bool:
        return isinstance(other, From) and self.val == other.val

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} val=({self.val})>"

    def __bool__(self) -> bool:
        return self.val is not None

    def __lshift__(self, method: Callable[[T], U]) -> From[U]:
        return self.bind(method)

    def __and__(self, method: Callable[[T], Any]) -> From[T]:
        return self.effect(method)

    @classmethod
    def unit(cls, val: U) -> From[U]:
        return cls(val)

    def bind(self, func: Callable[[T], U]) -> From[U]:
        return self.unit(func(self.val))

    def effect(self, func: Callable[[T], Any]) -> From[T]:
        func(self.val)
        return self


class Just(From[T]):
    pass


class Nothing(From[None]):
    pass


class Result(From[T]):
    """Result monad — catches exceptions and wraps them as Err."""

    def bind(self, func: Callable[[T], U]) -> Ok[U] | Err:
        try:
            return Ok(func(self.val))
        except Exception as e:
            return Err(e)


class Ok(Result[T]):
    pass


class Err(Result[Exception]):
    pass


# ---------------------------------------------------------------------------
# IO monad — chains side-effectful MeTTa operations
# ---------------------------------------------------------------------------


@dataclass
class IOStep:
    """A single recorded step in the IO chain."""

    name: str
    input: Any
    output: Any
    success: bool


@dataclass
class IO(From[T]):
    """IO monad that records each effectful step for traceability.

    Usage:
        result = (
            IO("some natural language")
            << parse_step
            << validate_step
            << store_step
        )
        result.val   # final value
        result.log   # list of IOStep records
    """

    val: T = None
    log: list[IOStep] = field(default_factory=list)

    def __repr__(self) -> str:
        status = "ok" if self else "empty"
        return f"<IO[{status}] steps={len(self.log)} val={self.val!r}>"

    @classmethod
    def unit(cls, val: U, log: list[IOStep] | None = None) -> IO[U]:
        return cls(val=val, log=log or [])

    def bind(self, func: Callable[[T], U]) -> IO[U]:
        step_name = getattr(func, "__name__", str(func))
        try:
            output = func(self.val)
            step = IOStep(name=step_name, input=self.val, output=output, success=True)
            logger.debug("io_step", step=step_name, success=True)
            return IO.unit(output, [*self.log, step])
        except Exception as e:
            step = IOStep(name=step_name, input=self.val, output=str(e), success=False)
            logger.warning("io_step_failed", step=step_name, error=str(e))
            return IO.unit(None, [*self.log, step])

    def effect(self, func: Callable[[T], Any]) -> IO[T]:
        step_name = getattr(func, "__name__", str(func))
        try:
            func(self.val)
            step = IOStep(name=step_name, input=self.val, output=None, success=True)
            self.log.append(step)
        except Exception as e:
            step = IOStep(name=step_name, input=self.val, output=str(e), success=False)
            self.log.append(step)
        return self

    @property
    def failed_steps(self) -> Sequence[IOStep]:
        return [s for s in self.log if not s.success]

    @property
    def succeeded(self) -> bool:
        return all(s.success for s in self.log)

    def summary(self) -> dict[str, Any]:
        return {
            "steps": len(self.log),
            "succeeded": self.succeeded,
            "value": self.val,
            "trace": [
                {"name": s.name, "success": s.success, "output": s.output}
                for s in self.log
            ],
        }
