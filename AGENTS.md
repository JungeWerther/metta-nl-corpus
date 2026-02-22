# Agent Guidelines

**metta-nl-corpus** is a labeling pipeline for the MeTTa-NL-Corpus. It ingests datasets (e.g., from HuggingFace), runs annotation via local or remote models (e.g., Ollama with gemma3), and outputs labeled data. The pipeline is orchestrated with Dagster and can be run via CLI (`python main.py run`) or the Dagster UI.

For project coding standards and conventions, see [.claude/rules.md](.claude/rules.md).

## Hooks

- After modifying code, re-run the pipeline via the CLI (`uv run python main.py run`) to verify changes
- At the end of a coding session, run the test suite to ensure tests are not failing
