Classify and store `$ARGUMENTS` under an ontology.

Input is just natural language — no prefix needed. (e.g. `/ontology how are you?`)

Do NOT explain or reason. Run a single bash heredoc script that:

1. Pick the best upper ontology leaf class for the input (from the valid list below)
2. Convert `<input>` to MeTTa expressions (you write them inline)
3. Store via `subprompt()`
4. Classify primary predicate: `(<Ontology> <Predicate>)`
5. Store classification via `subprompt()`
6. Walk `is-a` chain from `<Ontology>` up to `Entity` via `_cached_space`

Template (fill in O, I, E — you choose O automatically):

```bash
PETTA_PATH=~/sites/PeTTa uv run python << 'PYEOF'
from metta_nl_corpus.mcp_server import subprompt, _cached_space
from metta_nl_corpus.lib.helpers import parse_all
import json
O='<Ontology>';I='<input>';E='<your expressions>'
r1=subprompt(I,E,'claude-opus-4-6')
p=str(parse_all(E)[0]).strip('()').split()[0]
c=f'({O} {p})';r2=subprompt(f'{p} is a {O}',c,'claude-opus-4-6')
runner=_cached_space._ensure_runner()
b='!'
[runner.run(f'{b}(add-proposition {a})') for a in parse_all(E+chr(10)+c)]
# Walk is-a chain dynamically from O up to Entity
chain=[];cur=O
while cur and cur != 'Entity':
    ev=runner.run(f'{b}(find-evidence-for ({cur} {p}))')
    if ev and ev[0]: chain.append(f'{cur}: {ev[0][0]}')
    parent=runner.run(f'{b}(match &self (is-a {cur} $parent) $parent)')
    if parent and parent[0]:
        raw=str(parent[0][0]) if isinstance(parent[0], list) else str(parent[0])
        cur=raw.strip('[]')
    else:
        cur=None
if cur=='Entity':
    ev=runner.run(f'{b}(find-evidence-for (Entity {p}))')
    if ev and ev[0]: chain.append(f'Entity: {ev[0][0]}')
print(json.dumps({'input':I,'expressions':E,'classification':c,'chain':chain},indent=2))
PYEOF
```

Show only:
```
<Ontology> : <input>
expressions: ...
classification: (<Ontology> <Predicate>)
chain: [proof chain up to Entity]
```

Valid upper ontology classes (pick the most specific leaf):
- Continuant: Object, Agent, CognitiveAgent, SoftwareAgent, Artifact, InformationArtifact, Quality, Role, Disposition
- Occurrent: Process, Event, State, IntentionalProcess, CommunicationProcess, ComputationalProcess, PhysicalProcess, Action
- Abstract: Proposition, Description, Plan, Goal
