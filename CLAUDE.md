# CLAUDE.md

## Product Definition

MeTTa-NL-Corpus is a data pipeline that generates and validates Natural Language to MeTTa expression pairs. It takes premise-hypothesis pairs from NLI datasets (currently SNLI), uses LLMs to convert them into formal MeTTa s-expressions, and validates the results using the MeTTa inference engine.

**Goal**: Produce silver (20k AI-generated, probabilistically verified) and gold (10k human-verified) datasets of labeled NL-MeTTa pairs.

## Architecture

```
SNLI Dataset -> Preprocessing -> LLM Generation -> MeTTa Validation -> Annotated Parquet
```

- **Orchestration**: Dagster assets (ingestion + transformation)
- **LLM backends**: Ollama (local, default: gemma3:1b) and OpenAI (cloud, gpt/o1 models)
- **Validation**: MeTTa inference engine with separate entailment and contradiction spaces
- **Data**: Polars DataFrames, Pandera schema validation, Parquet storage
- **CLI**: Click-based entry point (`python main.py run`)

## Key Modules

- `main.py` — CLI entry point
- `metta_nl_corpus/models/` — Data models: TrainingData, Annotation, Validation, RelationKind
- `metta_nl_corpus/lib/` — Pipeline config, helpers (Box monad, MeTTa parsing), caching, space versioning
- `metta_nl_corpus/services/pipeline_executor.py` — Main orchestrator
- `metta_nl_corpus/services/defs/ingestion/assets.py` — Data loading from HuggingFace
- `metta_nl_corpus/services/defs/transformation/assets.py` — LLM generation, MeTTa parsing, validation
- `metta_nl_corpus/services/spaces/` — MeTTa grounding spaces (inference.metta, contradictions.metta)
- `documentation/annotation_guideline.md` — MeTTa conversion principles (used as LLM system prompt)

## Validation Logic

Three validation paths based on label:
- **Entailment**: Can the hypothesis be derived from the premise via transitive reasoning? (inference.metta)
- **Contradiction**: Do premise + hypothesis produce logical bottom? (contradictions.metta)
- **Neutral**: Neither entailment nor contradiction holds

Validation records are decoupled from annotations — annotations can be re-validated as the MeTTa spaces improve, tracked via MD5/git hashes.

## Running

```bash
uv run python main.py run --subset-size 50 --batch-size 10  # Ollama
uv run python main.py run --annotation-model gpt-4           # OpenAI
uv run dagster dev                                            # Dagster UI
uv run pytest tests/ -v                                       # Tests
```

## Coding Rules

See `.claude/rules.md` for coding conventions: StrEnum for constants, structlog (never print), typed everything with NamedTuple returns, abstract types (Mapping/Sequence).
