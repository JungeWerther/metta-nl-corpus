Classify and store `$ARGUMENTS` under an ontology.

Format: `<Ontology> : <input>` (e.g. `SpeechAct : how are you?`)

Do NOT explain, reason, or use MCP tools. Run a single `uv run python -c "..."` that does everything:

1. Convert `<input>` to MeTTa expressions (you write them inline in the script)
2. Store expressions via `subprompt()`
3. Classify the primary predicate: `(<Ontology> <Predicate>)`
4. Store the classification via `subprompt()`
5. Prove via `JanusPeTTaRunner` + `find-evidence-for`
6. Print JSON result

Template (fill in ONTOLOGY, INPUT, EXPRESSIONS):

```
PETTA_PATH=~/sites/PeTTa uv run python -c "
from metta_nl_corpus.mcp_server import subprompt
from metta_nl_corpus.lib.helpers import parse_all
from metta_nl_corpus.lib.runner import JanusPeTTaRunner
import json
O='<Ontology>'; I='<input>'; E='<expressions>'
r1=subprompt(I,E,'claude-opus-4-6')
p=str(parse_all(E)[0]).strip('()').split()[0]
c=f'({O} {p})'; r2=subprompt(f'{p} is a {O}',c,'claude-opus-4-6')
runner=JanusPeTTaRunner(); runner.load_file('metta_nl_corpus/services/spaces/inference-petta.metta')
[runner.run(f'!(add-proposition {a})') for a in parse_all(E+chr(10)+c)]
ev=runner.run(f'!(find-evidence-for {c})')
print(json.dumps({'expressions':E,'classification':c,'proof':ev[0][0] if ev and ev[0] else 'none','ids':[r1.get('value'),r2.get('value')]},indent=2))
"
```

Show only:
```
<Ontology> : <input>
expressions: ...
classification: (<Ontology> <Predicate>)
proof: <evidence>
```
