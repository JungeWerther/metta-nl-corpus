# metta-nl-corpus
Labeling pipeline for MeTTa-NL-Corpus

## Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- [Ollama](https://ollama.com/download) installed and running
- The `gemma3:1b` model available in Ollama:
  ```bash
  ollama pull gemma3:1b
  ```

## Setup

1. Install dependencies using `uv`:
   ```bash
   uv sync
   ```

2. Install pre-commit hooks:
   ```bash
   uv run pre-commit install
   ```

   Pre-commit hooks will automatically run on each commit to check code quality and formatting. The hooks include:
   - YAML validation
   - End-of-file fixes
   - Trailing whitespace removal
   - Ruff linting and formatting

   You can also manually run the hooks:
   ```bash
   uv run pre-commit run --all-files
   ```

## Running the Project

Make sure Ollama is running before starting the pipeline:
```bash
ollama serve
```

Then start the Dagster UI in a separate terminal:
```bash
uv run dagster dev
```

This will start the Dagster web UI at `http://localhost:3000` where you can view and execute the pipeline assets.

## Contributing

When creating pull requests, please use the following branch naming convention:

- Format: `{firstName}/{feature}`
- Examples:
  - `john/add-new-asset`
  - `sarah/fix-ingestion-bug`
  - `mike/update-documentation`

This helps organize branches and makes it easier to identify who is working on what feature.
