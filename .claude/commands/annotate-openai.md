Batch-annotate unannotated SNLI pairs via the lightweight CLI (no subprocess validation).

Run with optional arguments: `/annotate-openai 500` (default: 500), `/annotate-openai 200 offset 8000`, `/annotate-openai 100 model openai:gpt-4o`.

Parse `$ARGUMENTS`: extract a number for `total` (default 500). If the word `offset` appears, take the number after it as `offset` (default 5500). If the word `model` appears, take the string after it as `model` (default `openai:gpt-5-nano`). If the word `label` appears, take the string after it as `label` (omitted by default).

## 1. Compute batch parameters

Batch size is always 25. Compute `num_batches` = ceil(`total` / 25).

## 2. Run the annotate CLI command

Execute via Bash:

```
uv run python main.py annotate --model <model> --batch-size 25 --num-batches <num_batches> --offset <offset>
```

Append `--label <label>` only if a label was specified.

Stream the output so progress is visible. Use a 10-minute timeout since large batches take time.

## 3. Report results

After the command completes, summarize the structlog output:
- Total pairs stored
- Total valid
- Estimated cost
- Any errors or warnings

Then query the DB for the new total: call `mcp__metta-nl-corpus__query_annotations` with `file: "annotations"` and `limit: 1` to get the total count.
