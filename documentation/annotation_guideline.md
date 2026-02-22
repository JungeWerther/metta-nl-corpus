# Metta-nl-corpus
__Reference document for Natural language <> MeTTa experessions.__

This is a basic guideline outlining some principles for the conversion of natural language to s-expressions in MeTTa.

## CRITICAL RULE: Parentheses are mandatory

**EVERY expression MUST be wrapped in parentheses** following the form `(predicate subject)` or `(predicate subject object)`. Bare tokens without parentheses are **INVALID**.

❌ **WRONG**: `jumpedOver a-person broken-down-airplane`
✅ **CORRECT**: `(jumpedOver a-person broken-down-airplane)`

❌ **WRONG**: `blue a-wizard`
✅ **CORRECT**: `(blue a-wizard)`

If it's not wrapped in parentheses, it's not a valid MeTTa Atom. Every single fact, predicate, or relation you produce must be an s-expression `(...)`.

## Basics
Recall that an s-expression is any expression of the form `(x1 x2 ... xn)` where `x_i` can be replaced with an arbitrary value, which may itself be an s-expression. Any such expression is refered to here as an `Atom`.

In MeTTa, we take the convention that predication is the same as assigning an object to a class. For example, if we want to express that "Socrates is human", we can write:

```MeTTa
(human Socrates)
```

Let's say I wanted to make a basic assertion, or "predicate" something about an Atom. For example, imagine I wanted to say that "Socrates has black hair".
One way of expressing this in MeTTa would be so assign the property of "blackhairedness" to `Socrates`:

```MeTTa
; (Predicate Object)
(blackHaired Socrates)

; equivalently:
(=> (Socrates) (blackHaired))
```

__Both expressions are valid!__

# Definite versus indefinite articles

In the case of Socrates, it's pretty obvious who we're talking about. Socrates can only have one meaning, because it's not only a _name_, and we all know who we're talking about. So two statements mentioning Socrates are probably talking about the same person.
Generally speaking though, it won't be possible to make such assignations.

Before representing expressions in MeTTa, always think about whether we're talking about a particular or a general concept.

**Rule of thumb**: Whenever we're talking about a particular, we give a name prefixed with _this_. For example:

When asked to represent "a woman walks in the park. There are no women in the park". Write:

```MeTTa
(woman a-woman) ; "a-woman is a woman"
(not (inThePark woman)) ; "for all women x there is no x which has the property of being in the park"
```

**IMPORTANT**: Always add as many expressions as you like to capture all the concepts.

Examples:

```MeTTa
; the cat jumped off the roof
(cat the-cat) ; being a cat is a property of the-cat
(jumpedOffRoof the-cat)

; some elephant in the room
(elephant some-elephant)
(inTheRoom some-elephant)

; I knew that John was angry
(=> (John) (human)) ; same as (human John)
(=> myKnowledge (angry John))

; it was a day to remember
(day the-day) ; the-day is a day
(wasMemorable the-day) ; wasMemorable is a property of the-day

; a blue wizard appeared suddenly
(wizard a-wizard)
(blue a-wizard)
(suddenlyAppeared a-wizard)
```

Some notes:
- whenever multiple properties are mentioned, simply add them as separate Atom expressions

## Quantification, negation, products

Great! What about sentences like "all swans are white" or "there exists a black swan"? We handle them just in the same way as before. But we simply add the predicates the generic class!

```MeTTa
; all swans are white
(=> (swan) (white))

; there exists a black swan
(swan some-swan) ; being a swan is a property of some-swan
(exists some-swan)
(black some-swan)
```

When we're dealing with a negation, we use the `is-not` keyword. This is **critical** because the inference engine uses `is-not` to detect contradictions. **Never use `not` for negation — always use `is-not`.**

There are three valid negation forms (all equivalent):

```MeTTa
; "entity does NOT have property" — use ONE of these forms:
((is-not property) entity)   ; preferred form
(is-not (property entity))   ; also valid
(is-not property entity)     ; also valid
```

Examples:

```MeTTa
; there is not a hair on my head that considers this
(hair hair-on-my-head) ; hair-on-my-head is a hair
(on-my-head hair-on-my-head) ; hair-on-my-head is on my head
((is-not considers-this) hair-on-my-head)  ; hair-on-my-head does NOT consider this

; the person is not outdoors
((is-not outdoors) a-person)

; the swan is not black
((is-not black) some-swan)
```

```MeTTa
; Kayley's head is blue and red

(woman kayley)
(blue-head kayley)
(red-head kayley)

; equivalent:
(, (blue-head kayley) (red-head kayley))
```

## Entailment

Entailment is how we check whether a hypothesis logically follows from a set of premises. In MeTTa, entailment works through **transitivity** of the `=>` (implication) relation.

The key idea: when we have `(white swan)` in the space (meaning "swans are white") and `(swan this-swan)` (meaning "this-swan is a swan"), the inference engine derives `(white this-swan)` via transitivity. This is because `(white swan)` is expanded into `(=> ($x swan) ($x white))` — anything that is a swan is also white. Since `(swan this-swan)` matches, we get `(white this-swan)`.

For entailment to hold, the **premise** expressions must allow deriving the **hypothesis** expressions through inference chains.

### Worked example

**Premise**: "All swans are white. This is a swan."
**Hypothesis**: "This is white."

Step 1 — Represent the premise:
```MeTTa
(white swan)        ; all swans are white
(swan this-swan)    ; this-swan is a swan
```

Step 2 — Represent the hypothesis:
```MeTTa
(white this-swan)   ; this-swan is white
```

Step 3 — Check entailment: When `(white swan)` is added to the space, the inference engine creates the rule `(=> ($x swan) ($x white))`. Since `(swan this-swan)` is in the space, the engine matches `$x = this-swan` and derives `(white this-swan)`. The hypothesis is derivable — **entailment holds**.

## Contradiction

A contradiction (⊥) is derived when the space contains **both** a positive and a negative assertion about the same property and entity.

### CRITICAL: Use 2-element predicates only

The contradiction rules ONLY work with **2-element expressions** of the form `(predicate entity)`. Multi-element predicates like `(on a-person horse)` will **NOT** trigger contradictions.

❌ **WRONG** (3 elements — contradiction will NOT be detected):
```MeTTa
(on a-person horse)
((is-not on) a-person horse)
```

✅ **CORRECT** (2 elements — contradiction WILL be detected):
```MeTTa
(onHorse a-person)
((is-not onHorse) a-person)
```

Always use **compound predicate names** (e.g., `onHorse`, `atDiner`, `ridingBicycle`) instead of multi-argument relations (e.g., `on ... horse`, `at ... diner`).

### Negation with `is-not`

There are three valid negation forms (all equivalent, all require 2-element base form):

```MeTTa
; "entity does NOT have property" — use ONE of these:
((is-not property) entity)   ; preferred form
(is-not (property entity))   ; also valid
(is-not property entity)     ; also valid
```

When **any** of these coexists with `(property entity)` in the space, the inference engine derives ⊥ (contradiction).

**IMPORTANT**: For contradiction detection, **both** the premise AND hypothesis expressions are added to the same space, and we check if ⊥ can be derived. The entities **MUST** be the same between premise and hypothesis.

### Worked example

**Premise**: "A person is on a horse."
**Hypothesis**: "The person is not on a horse."

Step 1 — Represent the premise (use compound predicate):
```MeTTa
(person a-person)
(onHorse a-person)
```

Step 2 — Represent the hypothesis (negate a premise property):
```MeTTa
(person a-person)
((is-not onHorse) a-person)
```

Step 3 — Check contradiction: Both are added to the space. The space contains `(onHorse a-person)` and `((is-not onHorse) a-person)`. The engine matches:
```MeTTa
(=> (, ($a $x) ((is-not $a) $x)) ⊥)
```
With `$a = onHorse` and `$x = a-person`, ⊥ is derived — **contradiction holds**.

### How to handle semantic contradictions

Often the premise and hypothesis are semantically contradictory but describe different actions (e.g., "walking" vs "riding"). To express this as a formal contradiction, the hypothesis MUST **negate a property from the premise** using the same entity:

**Premise**: "A person is riding a horse."
**Hypothesis**: "A person is at a diner." (contradicts being on a horse)

```MeTTa
; Premise
(person a-person)
(ridingHorse a-person)

; Hypothesis — negate the premise property
(person a-person)
((is-not ridingHorse) a-person)
```

### Entity alignment matters

If the entities don't match, no contradiction is found:

```MeTTa
(mortal Socrates)
((is-not mortal) Plato)
```

Here `$x` would need to be both `Socrates` and `Plato` simultaneously, which is impossible — **no contradiction**.

## Reference: Inference Engine

Below is the full content of the MeTTa inference engine (`inference.metta`). This is the actual code that powers entailment and contradiction detection in the pipeline:

```MeTTa

;; Let the space &a denote our propositions.
;;
;; 1. The relation (=> A B) satisfies:
;;  "whenever C => A, then C => B" (transitivity)

!(bind! &a (new-space))

;; 2. (all ...) can be used in three ways:
;;
;; - (all): returns everything in the space &a
;; - (all $expr): returns everything $expr or everything which directly 'implies' $expr
;;  -> for example, if &a contains [(white), (=> swan white)], then
;;  -> (all white) will return [(white), (swan)]
;;  :  "white is white" "swan is white"
;; - (all $expr $out): returns all everything that => $expr, represented as $out

(= (all) (match &a $x $x))
(= (all $expr) (match &a $expr $expr))
(= (all $expr $out) (match &a $expr $out))

;; 3. find proofs for $to-prove
(= (find-evidence-for $to-prove) (match &a $to-prove $to-prove))
(= (find-evidence-for $to-prove) (=>
  (let $hypothesis (match &a (=> $x $to-prove) $x)
      (find-evidence-for $hypothesis)
  )
  $to-prove
))
(= (find-evidence-for (, $x $y)) (, (find-evidence-for $x) (find-evidence-for $y)))


;; 4. adding propositions
(= (add-proposition $x) (add-atom &a $x)) ; do we really need the base atom, or only its relations?
(= (add-proposition ($attr $ob)) (add-atom &a (=> ($ob $x) ($attr $x))))


!(add-proposition (=>
  (,
    ($a $x)
    ((is-not $a) $x)
  )
  ⊥
))

!(add-proposition (=>
  (,
    ($a $x)
    (is-not ($a $x))
  )
  ⊥
))


!(add-proposition (=>
  (,
    ($a $x)
    (is-not $a $x)
  )
  ⊥
))

(= (add-proposition ($rel $ob $sub)) (add-atom &a (=> ($x $ob) ($rel $x $sub))))
(= (add-proposition ($rel $ob $sub)) (add-atom &a (=> ($x $sub) ($rel $ob $x))))
```

When generating a MeTTa expression, always wrap your final result in a single MeTTa code block: ```MeTTa\n```.
