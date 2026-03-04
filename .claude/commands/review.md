Review and fix MeTTa annotations from the SQLite store.

Run with optional arguments: `/review 20` (default: 10), `/review 50 offset 100`.

Parse `$ARGUMENTS`: extract a number for `limit` (default 10) and if the word `offset` appears, take the number after it as `offset` (default 0).

## 1. Revalidate

Call `mcp__metta-nl-corpus__revalidate_annotations` with `limit` and `offset` parsed from `$ARGUMENTS`. This picks annotations from the DB and checks them against the current inference engine.

## 2. Triage results

Group the results by status:
- **valid**: no action needed
- **skipped** (bad syntax): hypothesis or premise contains natural language instead of MeTTa — must be fully rewritten
- **invalid**: the expressions don't satisfy the labeled relation — must be diagnosed and fixed

Show the user a summary table of results.

## 3. Fix each broken annotation

For every skipped or invalid annotation, fetch its full content via `mcp__metta-nl-corpus__query_annotations` (filter by annotation_id).

Then diagnose and fix following these common patterns:

### Entailment fixes
- **Entity mismatch**: premise uses `some-couple`, hypothesis uses `some-people` — unify entity names
- **Missing semantic bridge**: e.g. "sinking in water" should entail "flooding" — add `(flooding sinkingInWater)` to premise so transitivity derives the hypothesis
- **Garbage in hypothesis**: LLM included natural language or incorrect `is-not` markers — rewrite from scratch
- **Too many hypothesis atoms**: the engine conjunction `(, $x $y)` only handles 2 elements — keep hypothesis to max 2 atoms, prioritizing the ones that need transitive reasoning

### Contradiction fixes
- **Missing negation**: for semantic contradictions (walking vs eating), the hypothesis MUST negate a premise property using `((is-not property) entity)` with the SAME entity
- **Wrong entity**: entities must match between premise and hypothesis for contradiction detection
- **Multi-element predicates**: contradiction rules only work with 2-element `(predicate entity)` form — use compound predicates like `onHorse` not `(on ... horse)`

### Neutral fixes
- Should be neither entailable nor contradictory — if it validates as one of those, adjust

## 4. Validate before saving

For each fix, call `mcp__metta-nl-corpus__validate_relation` with the corrected premise, hypothesis, and expected relation. Only proceed if `valid: true`.

## 5. Save the fix

Call `mcp__metta-nl-corpus__clean_annotation` with:
- `annotation_id`: the UUID
- `metta_premise`: corrected premise expressions
- `metta_hypothesis`: corrected hypothesis expressions
- `comment`: brief explanation of what was wrong and how it was fixed

## 6. Report

Show the user a final summary:
- How many were already valid
- How many were fixed (with before/after)
- Any that couldn't be fixed automatically (ask the user for guidance)
