Annotate unannotated SNLI pairs with MeTTa expressions.

Run with optional arguments: `/annotate 50` (default: 50), `/annotate 100 offset 20000`.

Parse `$ARGUMENTS`: extract a number for `limit` (default 50) and if the word `offset` appears, take the number after it as `offset` (default 10000).

## 1. Plan agent batches (no fetching in main context)

Divide `limit` into batches of 25. For each batch, compute the batch-specific `offset` and `limit`:
- Batch 0: offset=`offset`, limit=25
- Batch 1: offset=`offset + 25`, limit=25
- Batch 2: offset=`offset + 50`, limit=25
- etc. (last batch may have limit < 25)

Launch one parallel agent per batch. Do NOT call `yield_unannotated_pairs` in the main context — each agent fetches its own batch.

## 2. Each agent's task

The agent first calls `mcp__metta-nl-corpus__yield_unannotated_pairs` with its batch-specific `offset` and `limit` to fetch its own pairs. Then for every pair returned, the agent must:

### Generate MeTTa expressions

Follow the annotation guideline strictly. Key rules:

#### Entailment
Build transitive chains so the hypothesis is DERIVABLE from the premise:
- Use the same entity names in premise and hypothesis
- Add semantic bridges: `(derived-property base-property)` in the premise so `(base-property entity)` + bridge → `(derived-property entity)`
- Keep hypothesis to max 2 atoms in a conjunction `(, atom1 atom2)`

#### Contradiction
The hypothesis MUST negate a premise property:
- Use compound 2-element predicates ONLY: `(predicate entity)`, never `(pred arg1 arg2)`
- Hypothesis uses `((is-not property) entity)` with the SAME entity from the premise
- For semantic contradictions (walking vs eating), negate the premise action

#### Neutral
Hypothesis introduces NEW information that is neither derivable nor contradictory:
- Do NOT reuse premise predicates in hypothesis
- Do NOT use `is-not` on any premise property

### Validate before saving

Call `mcp__metta-nl-corpus__validate_relation` with:
- `metta_premise`: the generated premise expressions
- `metta_hypothesis`: the generated hypothesis expressions
- `relation`: the label from the SNLI pair
- `premise`: the NL premise
- `hypothesis`: the NL hypothesis
- `model`: "claude-opus-4-6"
- `store_result`: **false** (validate only)

If `valid: false`, try fixing once (common issues: entity mismatch, missing bridge, wrong negation form). If still invalid, skip and note it.

### Save valid annotations

Call `mcp__metta-nl-corpus__validate_relation` again with `store_result: true` to persist the annotation.

### Report

At the end, the agent reports: how many annotated successfully, how many skipped.

## 3. Final summary

After all agents complete, show the user:
- Total pairs processed
- Successfully annotated (with label breakdown)
- Skipped/failed (list the NL pairs that couldn't be annotated)
- New total annotation count in the DB
