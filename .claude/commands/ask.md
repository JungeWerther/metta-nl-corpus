Answer the open-ended question "$ARGUMENTS" by querying the personal knowledge base.

Do NOT explain or reason. Run a single bash heredoc script that:

1. Uses semantic search (`search_knowledge`) to find relevant annotations
2. Loads those expressions into the MeTTa space
3. Converts the question to an open-ended MeTTa query using `$r` variables
4. Runs `!(find-evidence-for ...)` to discover all provable facts
5. Deduplicates and formats the results

```bash
PETTA_PATH=~/sites/PeTTa uv run --extra petta python << 'PYEOF'
from metta_nl_corpus.mcp_server import search_knowledge, _cached_space, store
from metta_nl_corpus.lib.helpers import parse_all
import sqlite3, json
from metta_nl_corpus.constants import ANNOTATIONS_DB_PATH

Q = "$ARGUMENTS"

# Phase 1: Semantic search for relevant context
hits = search_knowledge(Q, field="premise", top_k=15, min_score=0.25)
relevant = hits.get("results", [])

# Phase 2: Load expression annotations + search hits into space
conn = sqlite3.connect(ANNOTATIONS_DB_PATH)
rows = conn.execute(
    "SELECT metta_premise FROM annotations WHERE label='expression' AND metta_premise IS NOT NULL"
).fetchall()
runner = _cached_space._ensure_runner()
loaded = set()
for (m,) in rows:
    for atom in parse_all(m):
        s = str(atom)
        if s not in loaded:
            runner.run(f'!(add-proposition {atom})')
            loaded.add(s)
for hit in relevant:
    mp = hit.get("metta_premise")
    if mp:
        for atom in parse_all(mp):
            s = str(atom)
            if s not in loaded:
                runner.run(f'!(add-proposition {atom})')
                loaded.add(s)

# Phase 3: Convert question to open-ended queries
# Extract key entities from the question
words = Q.lower().replace("?", "").replace("!", "").split()
stop = {"is", "are", "was", "were", "do", "does", "did", "can", "could",
        "what", "who", "where", "when", "why", "how", "the", "a", "an",
        "there", "about", "tell", "me", "know", "you", "i", "my", "and",
        "or", "of", "in", "on", "to", "for", "with", "it", "that", "this",
        "have", "has", "had", "be", "been", "being", "not", "no", "all", "any"}
entities = [w for w in words if w not in stop and len(w) > 1]

# Also extract entity names from semantic search hits (MeTTa atoms)
import re
_VALID_ATOM = re.compile(r'^[A-Za-z][A-Za-z0-9_-]*$')
for hit in relevant:
    mp = hit.get("metta_premise", "")
    if mp:
        for atom in parse_all(mp):
            for tok in str(atom).strip("()").split():
                if tok.startswith("$") or tok in ("is-a", ":", "=", "->", "is-not"):
                    continue
                if _VALID_ATOM.match(tok) and tok not in entities:
                    entities.append(tok)

# Build name variants: singular, plural strip, a-prefixed, capitalized
def variants(word):
    w = word.rstrip("s") if word.endswith("s") and len(word) > 3 else word
    return list(dict.fromkeys([
        word, w, f"a-{w}", f"a-{word}", f"the-{w}", f"the-{word}",
        word.capitalize(), w.capitalize(),
    ]))

# Build queries: try each entity as subject with open predicate
results = {}
for entity in entities:
    for name in variants(entity):
        if not _VALID_ATOM.match(name):
            continue
        try:
            r = runner.run(f'!(find-evidence-for ($r {name}))')
            if r and r[0]:
                for proof in r[0]:
                    ps = str(proof)
                    if ps not in results:
                        results[ps] = name
            # Also try as predicate
            r2 = runner.run(f'!(find-evidence-for ({name} $x))')
            if r2 and r2[0]:
                for proof in r2[0]:
                    ps = str(proof)
                    if ps not in results:
                        results[ps] = name
        except Exception:
            continue

# Phase 4: Format output
search_ctx = [{"premise": h["premise"][:80], "score": h["score"]} for h in relevant[:5]]
# Deduplicate proofs: keep unique conclusions, skip verbose nested =>
seen = set()
clean_proofs = []
for proof_str in results:
    # Extract the core fact from proof chains
    core = proof_str
    if core not in seen:
        seen.add(core)
        clean_proofs.append(core)

print(json.dumps({
    "question": Q,
    "entities_detected": entities,
    "semantic_context": search_ctx,
    "expressions_loaded": len(loaded),
    "proofs": clean_proofs[:30],
}, indent=2))
PYEOF
```

Show results as:

```
? <question>

Relevant context (semantic search):
  <score> | <premise>

Provable facts:
  - <proof 1>
  - <proof 2>
  ...
```

If no proofs are found, say:

> Nothing provable yet. Would you like me to add knowledge via `/subprompt`?
