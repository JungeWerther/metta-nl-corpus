# Project Coding Rules

## Constants and Enumerations

- When defining more than one related constant, prefer using `StrEnum` from the `enum` module
- This provides better type safety and IDE support compared to plain string constants

Example:
```python
from enum import StrEnum

class Status(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
```

Instead of:
```python
STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
```

## Logging

- Never use `print()` for output
- Always use `logger` from `structlog` for all logging and output
- Import logger with: `from structlog import get_logger`
- Initialize logger with: `logger = get_logger(__name__)`

Example:
```python
from structlog import get_logger

logger = get_logger(__name__)

logger.info("Processing started", dataset_id="squad", count=10)
logger.error("Processing failed", error=str(e))
```

Instead of:
```python
print("Processing started")
print(f"Error: {e}")
```

## Type Annotations

- Always type all variables and provide return type annotations for all functions
- Never return dictionaries - use `NamedTuple` instead for structured return values
- Prefer abstract types like `Mapping` and `Sequence` over concrete types like `dict` and `list` whenever possible

Example:
```python
from typing import NamedTuple, Mapping, Sequence

class ProcessResult(NamedTuple):
    status: str
    count: int
    errors: Sequence[str]

def process_data(items: Sequence[str], config: Mapping[str, str]) -> ProcessResult:
    count: int = len(items)
    errors: list[str] = []

    return ProcessResult(
        status="success",
        count=count,
        errors=errors,
    )
```

Instead of:
```python
def process_data(items, config):
    count = len(items)
    errors = []

    return {
        "status": "success",
        "count": count,
        "errors": errors,
    }
```
