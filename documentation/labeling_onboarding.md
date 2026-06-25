# MeTTa-NL-Corpus — Human Labeling Onboarding

> **You're being invited to own the human-verification layer of a neuro-symbolic dataset.**
> This doc gives you the *why*, the *what*, and a concrete *path to your first labeled batch*. Read it in ~10 minutes; you'll know whether this is your kind of project by the end.

---

## TL;DR

We use LLMs to translate plain-English sentences into **formal MeTTa logic expressions**, then we run those expressions through a real inference engine to check they behave correctly. The machine-checked tier ("silver") scales cheaply. The part that makes the dataset *trustworthy* — and the part we want you to lead — is the **human-verified "gold" tier of 10,000 pairs**.

- **Repo:** https://github.com/JungeWerther/metta-nl-corpus
- **Source dataset:** [Stanford SNLI on Hugging Face](https://huggingface.co/datasets/stanfordnlp/snli) (`stanfordnlp/snli`)
- **Your mission:** stand up the human-labeling mechanism, then drive it to **10k verified pairs**.

---

## Why this project exists

Most "natural language → logic" datasets are **generated and never checked**. A model emits something that *looks* like logic, nobody runs it, and the errors get baked into whatever trains on it.

We're doing the opposite. Every pair is **executed against an inference engine**, so a label isn't an opinion — it's a *proof outcome*. That verification step is the moat: a corpus of NL↔logic pairs where the logic is known to actually fire correctly is rare and valuable, both as **training data** for models that emit verifiable formal reasoning and as an **evaluation benchmark** for how well today's models can.

### The pipeline

```
SNLI pairs ──▶ LLM generation ──▶ MeTTa parsing ──▶ inference-engine validation ──▶ labeled corpus
 (premise/      (English →          (s-expression    (entailment / contradiction /
  hypothesis     MeTTa s-expr)       sanity check)    neutral, checked by execution)
  + NLI label)
```

### Concrete example — "all swans are white"

The English universal becomes one atom:

```metta
(white swan)
```

The engine rewrites that into a transitive rule (`(=> (swan $x) (white $x))`), so given `(swan this-swan)` it *derives* `(white this-swan)`. We don't take the translation's word for it — we run it:

| Query | Engine result | Meaning |
|---|---|---|
| `(find-evidence-for (white this-swan))` | `[(=> (swan this-swan) (white this-swan))]` | ✅ entailment holds — proof found |
| `(find-evidence-for (white some-swan))` | `[]` | ❌ no proof — correctly *not* derivable |

That non-empty-vs-empty distinction **is** the label. Multiply it by tens of thousands of pairs and you have the corpus.

---

## The goal: silver and gold

| Tier | Size | How it's verified | Status |
|---|---|---|---|
| **Silver** | ~20k | AI-generated, **machine-verified** by the inference engine (probabilistic, fast, cheap) | pipeline built |
| **Gold** | **10k** | **Human-verified** subset — a person confirms the MeTTa faithfully captures the English *and* the engine's verdict matches the SNLI label | **this is where you come in** |

**Why gold needs a human.** The engine can tell you a translation is *internally consistent* (it derives what it should). It can't always tell you the translation is *faithful* to the English — that a clever-looking s-expression actually means what the sentence means. That judgment is the human's job, and it's what turns silver into gold.

**Why SNLI is the scaffold.** Each [SNLI](https://huggingface.co/datasets/stanfordnlp/snli) row is a `(premise, hypothesis, label)` triple where `label ∈ {entailment, contradiction, neutral}` — already human-annotated by Stanford. That gives us a **free ground-truth signal** to check translations against:

- **entailment** → hypothesis should be *derivable* from premise (`find-evidence-for` returns evidence)
- **contradiction** → premise + hypothesis should derive `⊥` (logical bottom)
- **neutral** → neither

When the engine's verdict matches SNLI's label, that's strong evidence the MeTTa is sound. Your gold review is the final arbiter on the ones that matter.

---

## What you'd actually own

You're not just clicking labels — you're **designing the mechanism, then running it**:

1. **Stand up the labeling surface.** The repo already ships the hard parts:
   - a thread-safe **SQLite annotation store** (`metta_nl_corpus/lib/storage.py`) with decoupled `annotations` and `validations` tables — meaning annotations can be **re-validated** as the engine improves, without re-doing human work;
   - an **HTTP API** (`uv run python main.py serve`, port `8090`) you can point a thin review UI at;
   - a **`review`** workflow/skill for inspecting and fixing annotations pulled from the store.

   Your call on the front-end: a minimal web UI, a spreadsheet-export round-trip, or a TUI — whatever gets a human to a verdict fastest.

2. **Define the gold rubric.** Codify "what counts as a faithful translation" into a short, repeatable checklist (the existing [`documentation/annotation_guideline.md`](./annotation_guideline.md) is the starting point — it doubles as the LLM's own system prompt, so humans and model share one rulebook).

3. **Label to 10k.** Review machine-verified pairs, accept / fix / reject, and promote the keepers to gold. The decoupled store means your verdicts persist even as the silver pipeline and inference spaces keep improving underneath you.

---

## Get it running in ~15 minutes

```bash
# 1. Clone
git clone https://github.com/JungeWerther/metta-nl-corpus.git
cd metta-nl-corpus

# 2. Install (uv handles the Python 3.12 env + deps, incl. the hyperon MeTTa engine)
uv sync

# 3. (optional, for local generation) Ollama + a small model
ollama pull gemma3:1b

# 4. Generate a tiny batch to see the full loop end-to-end
uv run python main.py run --subset-size 50 --batch-size 10

# 5. Bring up the API the labeling UI talks to
uv run python main.py serve            # http://localhost:8090

# 6. Explore assets / lineage visually
uv run dagster dev
```

SNLI downloads automatically on first run (cached from `stanfordnlp/snli`) — no manual data wrangling. Tests: `uv run pytest tests/ -v`.

---

## Next-step goals (your roadmap)

- [ ] **Labeling mechanism v1** — store-backed review surface where a human sees `(premise, hypothesis, SNLI label, generated MeTTa, engine verdict)` and records a gold decision.
- [ ] **Gold rubric** — a crisp accept/fix/reject checklist built on `annotation_guideline.md`.
- [ ] **First 500 gold pairs** — prove the workflow end-to-end and measure throughput (pairs/hour).
- [ ] **Disagreement triage** — prioritize pairs where engine verdict ≠ SNLI label; those are the highest-information items to review.
- [ ] **Scale to 10k** — with quality metrics (inter-annotator agreement once there's more than one labeler).
- [ ] **Publish** — release the gold split (and the engine-validation harness) so the dataset is citable and reusable.

---

## Why join

- **Rare problem space.** Neuro-symbolic AI sits at the intersection of LLMs and formal logic — verifiable reasoning is one of the most interesting open frontiers, and very few datasets do it *checked*.
- **Real ownership.** You're not labeling someone else's spec — you design the mechanism and the rubric, and the gold set carries your fingerprint.
- **The infrastructure is already built.** Inference engine, generation pipeline, storage, validation, API — all working. You get to focus on the highest-leverage layer instead of plumbing.
- **A tangible artifact.** A 10k human-verified NL↔MeTTa corpus is a publishable, citable contribution — useful to researchers and to anyone training models that reason in formal logic.

**Two links to start:** the [repo](https://github.com/JungeWerther/metta-nl-corpus) and the [SNLI dataset](https://huggingface.co/datasets/stanfordnlp/snli). Clone it, run the 50-pair batch, watch a sentence become a proof. If that's satisfying to you, you're the right person for the gold tier.
