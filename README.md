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

There are two ways to run the annotation pipeline:

### Option 1: Using the CLI (Recommended)

Make sure Ollama is running before starting the pipeline:
```bash
ollama serve
```

Then run the pipeline using the CLI:
```bash
# Run with default settings (10 samples, batch size 10)
uv run python main.py run

# Run with custom batch and subset sizes
uv run python main.py run --subset-size 100 --batch-size 20

# Run with a specific dataset
uv run python main.py run --hf-id squad --subset-size 50 --batch-size 10

# See all available options
uv run python main.py run --help
```

**Available CLI Options:**
- `--hf-id`: HuggingFace dataset ID (default: "squad")
- `--filename`: Dataset filename (default: "train.parquet")
- `--split`: Dataset split (default: "train")
- `--model-name`: Model name for the pipeline run (default: "example-model")
- `--version`: Version of the pipeline run (default: "v1")
- `--subset-size`: Number of samples to process (default: 10)
- `--batch-size`: Batch size for processing (default: 10)
- `--annotation-model`: Model to use for annotation generation (default: "gemma3:1b")

### Option 2: Using Dagster UI

Start the Dagster UI in a separate terminal:
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
