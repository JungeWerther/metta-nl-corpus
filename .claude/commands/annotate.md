Annotate unannotated SNLI pairs with MeTTa expressions.

Run with optional arguments: `/annotate 50` (default: 50), `/annotate 100 offset 20000`.

Parse `$ARGUMENTS`: extract a number for `limit` (default 50) and if the word `offset` appears, take the number after it as `offset` (default 10000).

## 1. Fetch unannotated pairs

Call `mcp__metta-nl-corpus__yield_unannotated_pairs` with `limit` and `offset` parsed from the arguments. This returns SNLI premise/hypothesis/label triples that are not yet in the annotation DB.

Show the user a summary: how many pairs returned, label distribution.

## 2. Split into agent batches

Write the returned pairs to `/tmp/annotate_pairs.json`. Split into batches of 25, and launch one parallel agent per batch.

## 3. Each agent's task

For every pair in the batch, the agent must:

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

## 4. Final summary

After all agents complete, show the user:
- Total pairs processed
- Successfully annotated (with label breakdown)
- Skipped/failed (list the NL pairs that couldn't be annotated)
- New total annotation count in the DB
