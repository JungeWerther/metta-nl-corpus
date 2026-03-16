Convert the following natural language into MeTTa expressions, then parse, validate, and store them via the IO monad pipeline:

"$ARGUMENTS"

Follow these steps:

## 1. Generate MeTTa expressions

Convert the sentence into MeTTa s-expressions following the annotation guideline. Remember:
- Every expression MUST be parenthesized: `(predicate subject)` or `(predicate subject object)`
- Use `a-thing` or `the-thing` naming for particulars
- Use bare class names for universals/categories
- Use `is-a` for type hierarchies
- Use `is-not` for negation (never bare `not`)
- Use compound predicates for 2-element form: `(onHorse a-person)` not `(on a-person horse)`
- Extract ALL concepts: explicit, implicit, properties, relations, inheritance

## 2. Validate syntax

Call `mcp__metta-nl-corpus__parse_metta` with the generated expressions to confirm they parse. Fix any errors.

## 3. Store via IO monad

Call `mcp__metta-nl-corpus__subprompt` with:
- `sentence`: the original sentence ("$ARGUMENTS")
- `metta_expressions`: all validated MeTTa expressions (newline-separated)
- `model`: "claude-opus-4-6"

This runs the full IO chain (parse -> log -> store) and returns a trace of each step.

## 4. Output

Show the user:
1. The MeTTa expressions in a code block
2. The IO trace summary (steps, success/failure)
3. The annotation_id confirming storage
