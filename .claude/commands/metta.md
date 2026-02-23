Analyze the following sentence and extract a full ontology as MeTTa s-expressions:

"$ARGUMENTS"

Follow these steps:

## 1. Ontology extraction

Identify ALL of the following from the sentence:
- **Explicit concepts**: every noun, entity, and named thing
- **Properties**: adjectives, states, qualities attributed to entities
- **Actions/relations**: verbs and relational predicates
- **Inheritance hierarchy**: what categories entities belong to (e.g. dog is-a animal, stick is-a object)
- **Dormant/implicit assumptions**: things that must be true for the sentence to make sense but aren't stated (e.g. "a dog fetches a stick" implies the dog is alive, the stick exists as a physical object, fetching involves movement)

## 2. Generate MeTTa expressions

Convert everything into MeTTa s-expressions following the annotation guideline. Remember:
- Every expression MUST be parenthesized: `(predicate subject)` or `(predicate subject object)`
- Use `a-thing` or `the-thing` naming for particulars
- Use bare class names for universals/categories
- Use `is-not` for negation (never bare `not`)
- Use compound predicates for 2-element form: `(onHorse a-person)` not `(on a-person horse)`
- Add as many expressions as needed to capture the full ontology

## 3. Validate syntax

Call `mcp__metta-nl-corpus__parse_metta` with all your generated expressions to confirm they parse correctly. If any fail, fix and re-validate.

## 4. Check logical consistency

For each derived/inherited fact, call `mcp__metta-nl-corpus__validate_relation` with:
- `metta_premise`: the base ontology expressions (the direct facts from the sentence)
- `metta_hypothesis`: the derived fact being checked
- `relation`: "entailment"

This confirms your inheritance chains actually work in the inference engine. If validation fails, adjust the expressions.

## 5. Save to annotations

Call `mcp__metta-nl-corpus__add_expressions` with:
- `sentence`: the original sentence
- `metta_expressions`: all validated MeTTa expressions (newline-separated)
- `model`: "claude" (or your model name)

## Output format

Show the user:
1. A summary of extracted concepts organized by category
2. The full set of MeTTa expressions in a code block
3. Validation results
4. Confirmation that expressions were saved
