Classify and store `$ARGUMENTS` under an ontology.

Format: `<Ontology> : <input>` (e.g. `CommunicationProcess : how are you?`)

Do NOT explain or reason. Run a single bash heredoc script that:

1. Converts `<input>` to MeTTa expressions (you write them inline)
2. Stores via `subprompt()`
3. Classifies primary predicate: `(<Ontology> <Predicate>)`
4. Stores classification via `subprompt()`
5. Proves the full transitive chain via `_cached_space` (auto-loads inference engine + upper ontology)

Template (fill in O, I, E):

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
chain=[]
for t in [O,'Process','Occurrent','Entity']:
    ev=runner.run(f'{b}(find-evidence-for ({t} {p}))')
    if ev and ev[0]: chain.append(f'{t}: {ev[0][0]}')
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

Valid upper ontology classes: Entity, Continuant, Occurrent, Abstract, Object, Agent, CognitiveAgent, SoftwareAgent, Process, Event, State, IntentionalProcess, CommunicationProcess, ComputationalProcess, PhysicalProcess, Action, Proposition, Description, Plan, Goal, Role, Quality, Disposition, Artifact, InformationArtifact
