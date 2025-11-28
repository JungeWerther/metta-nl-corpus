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
