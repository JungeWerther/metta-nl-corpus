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
(black-haired Socrates)
```

# Definite versus indefinite articles

In the case of Socrates, it's pretty obvious who we're talking about. Socrates can only have one meaning, because it's not only a _name_, and we all know who we're talking about. So two statements mentioning Socrates are probably talking about the same person.
Generally speaking though, it won't be possible to make such assignations.

Before representing expressions in MeTTa, always think about whether we're talking about a particular or a general concept.

**Rule of thumb**: Whenever we're talking about a particular, we give a name prefixed with _this_. For example:

When asked to represent "a woman walks in the park. There are no women in the park". Write:

```MeTTa
(woman a-woman) ; "a-woman is a woman"
(is-not (in-the-park woman)) ; "for all women x there is no x which has the property of being in the park"
```

**IMPORTANT**: Always add as many expressions as you like to capture all the concepts.

Examples:

```MeTTa
; the cat jumped off the roof
(cat the-cat) ; being a cat is a property of the-cat
(jumped-off the-cat the-roof)

; some elephant in the room
(elephant some-elephant)
(in-the-room some-elephant)

; I knew that John was angry
(human John)
((angry John) my-knowledge)

; it was a day to remember
(day the-day) ; the-day is a day
(was-memorable the-day) ; wasMemorable is a property of the-day

; a blue wizard appeared suddenly
(wizard a-wizard)
(blue a-wizard)
(suddenly-appeared a-wizard)
```

Some notes:
- whenever multiple properties are mentioned, simply add them as separate Atom expressions

## Type hierarchy with `is-a`

Use `(is-a particular Class)` to place entities in a type hierarchy, and `(is-a SubClass SuperClass)` to express class inheritance. This is useful for building rich ontologies.

```MeTTa
; a speaker who is an agent, and agents are things
(is-a a-speaker Agent)
(is-a Agent Thing)

; a want is a mental state
(is-a a-want Want)
(is-a Want MentalState)

; a dog is an animal, animals are living things
(is-a a-dog Dog)
(is-a Dog Animal)
(is-a Animal LivingThing)
```

**Note on `is-a` vs bare predication**: Both `(human Socrates)` and `(is-a Socrates Human)` express type membership. Use bare predication `(human Socrates)` for simple, flat assertions. Use `(is-a ...)` when building multi-level type hierarchies where the class structure itself matters.

### How `is-a` works in the inference engine

The inference engine includes the rewrite rule `(= (is-a $x $y) ($y $x))`, which converts `is-a` expressions into standard predication. This means `(is-a a-dog Dog)` is equivalent to `(Dog a-dog)`, and transitive chains work automatically:

```MeTTa
; Given:
(is-a a-dog Dog)      ; rewrites to (Dog a-dog)
(is-a Dog Animal)     ; rewrites to (Animal Dog), which expands to: anything that is a Dog is also an Animal

; The engine derives:
(is-a a-dog Animal)   ; (Animal a-dog) — via transitivity
```

Multi-level `is-a` hierarchies are fully supported for both ontology extraction and entailment validation.

## Modelling states, abilities, and causation

When sentences describe states, abilities, or causal relationships ("X so Y", "X therefore Y", "because X, Y"), model these as explicit relations between concepts.

### States and properties of agents

```MeTTa
; "I am free"
(is-a a-speaker Agent)
(free a-speaker)
(is-a Freedom State)
(hasState a-speaker Freedom)
```

### Abilities

```MeTTa
; "I can do whatever I want"
(is-a a-action Action)
(canDo a-speaker a-action)
(unrestricted a-action)
```

### Causal / enabling relations

When one fact enables or causes another, use `enables` or `causes`:

```MeTTa
; "I am free so I can do whatever I want"
(free a-speaker)
(hasState a-speaker Freedom)
(canDo a-speaker a-action)
(enables Freedom a-action)  ; freedom enables the action
```

### Mental states

Model wants, beliefs, knowledge, and intentions as typed entities with ownership relations:

```MeTTa
; "whatever I want"
(is-a a-want Want)
(is-a Want MentalState)
(wantsBy a-want a-speaker)  ; the want belongs to the speaker
```

## Quantification, negation, disjunction, products

Great! What about sentences like "all swans are white" or "there exists a black swan"? We handle them just in the same way as before. But we simply add the predicates the generic class!

### Universal quantification (`add-proposition`)

When a property applies to **all** members of a class, use `add-proposition`. This creates a universal rule in the engine — anything that is a member of the class inherits the property.

```MeTTa
; all swans are white (universal: every swan is white)
(white swan)
```

### Existential quantification (`add-existential`)

When a property applies to **some** members of a class, use `add-existential`. This adds a fact about a specific witness **without** creating a universal rule. "Some dogs are brown" should NOT entail "all dogs are brown".

```MeTTa
; some dogs are brown (existential: only this witness is brown)
(dog some-dog)
(brown some-dog)
```

**When to use which:**
- `add-proposition` — "all swans are white", "every man is mortal", "cats are animals"
- `add-existential` — "some dogs are brown", "there exists a black swan", "a few people were running"

### Disjunction (`or`)

When a sentence expresses alternatives ("X or Y"), use `(or A B)`. The inference engine proves `(or A B)` if it can prove **either** A or B.

```MeTTa
; "the person is running or walking"
(person a-person)
(or (running a-person) (walking a-person))

; "the animal is a cat or a dog"
(animal some-animal)
(or (cat some-animal) (dog some-animal))
```

**IMPORTANT**: `(or A B)` is used in the **hypothesis** to check if at least one alternative holds. In the **premise**, you typically assert the specific fact that is true (e.g., `(running a-person)`).

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

; it's also fine to present a synonym, when dealing with expressions
(is-not considered-by-me this)
```

```MeTTa
; Kayley's head is blue and red

(woman kayley)
(blue-head kayley)
(red-head kayley)

; equivalent:
(, (blue-head kayley) (red-head kayley))
```

## Ontology extraction

When extracting a full ontology from a sentence (as opposed to NLI premise/hypothesis pairs), aim to capture **everything** the sentence implies — not just what it literally says.

### What to extract

1. **Explicit concepts** — every noun, entity, named thing
2. **Properties** — adjectives, states, qualities attributed to entities
3. **Actions/relations** — verbs and relational predicates
4. **Type hierarchy** — what categories entities belong to (`is-a` chains)
5. **Implicit assumptions** — things that must be true for the sentence to make sense but aren't stated

### Worked example

**Sentence**: "I am free so I can do whatever I want"

Concepts: speaker (Agent), freedom (State), action, want (MentalState)
Relations: hasState, canDo, enables, wantsBy
Implicit: freedom is a state, agents have wants, freedom enables action

```MeTTa
; Type hierarchy
(is-a a-speaker Agent)
(is-a Agent Thing)
(is-a Freedom State)
(is-a a-want Want)
(is-a Want MentalState)
(is-a a-action Action)

; Direct properties
(free a-speaker)
(hasState a-speaker Freedom)
(unrestricted a-action)

; Relations
(canDo a-speaker a-action)
(wantsBy a-want a-speaker)
(enables Freedom a-action)
```

## Entailment

Entailment is how we check whether a hypothesis logically follows from a set of premises. In MeTTa, entailment works through **transitivity** of predication.

The key idea: when we have `(white swan)` in the space (meaning "swans are white") and `(swan this-swan)` (meaning "this-swan is a swan"), the inference engine derives `(white this-swan)` via transitivity. This is because `(white swan)` is internally expanded into a rule — anything that is a swan is also white. Since `(swan this-swan)` matches, we get `(white this-swan)`.

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

Step 3 — Check entailment: When `(white swan)` is added to the space, the inference engine creates a transitive rule. Since `(swan this-swan)` is in the space, the engine matches and derives `(white this-swan)`. The hypothesis is derivable — **entailment holds**.

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

Step 3 — Check contradiction: Both are added to the space. The space contains `(onHorse a-person)` and `((is-not onHorse) a-person)`. The engine matches with `$a = onHorse` and `$x = a-person`, ⊥ is derived — **contradiction holds**.

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

Here the engine would need `$x` to be both `Socrates` and `Plato` simultaneously, which is impossible — **no contradiction**.

## PLN Truth Values

Propositions can carry **Simple Truth Values** (STV) from PLN (Probabilistic Logic Networks). An STV has two components:

- **strength** ∈ [0.0, 1.0] — the probability that the proposition is true
- **confidence** ∈ [0.0, 1.0] — how much evidence supports that estimate

Use `add-proposition-tv` to assert a proposition with a truth value:

```MeTTa
;; "BTC price above 60k by May 1st" — 70% likely, moderate confidence
(≞ (> btc-price 60000) (STV 0.7 0.6))

;; "Dogs shed fur" — very high probability, well-established
(≞ (shedsFur Dog) (STV 0.97 0.95))
```

The TV layer is additive — propositions with TVs still participate in boolean inference (transitivity, contradiction detection). The STV is metadata that tracks uncertainty.

### Combining truth values

PLN deduction propagates uncertainty: `s = s1 × s2, c = min(c1, c2)`.

```MeTTa
!(combine-tv (STV 0.9 0.8) (STV 0.7 0.6))
;; => (STV 0.63 0.6)
```

### When to use TVs

- **Predictions**: "BTC above 60k" — `(STV 0.7 0.6)`
- **Empirical claims**: "Dogs shed fur" — `(STV 0.97 0.95)`
- **Mathematical certainties**: Use `(STV 1.0 1.0)` — these are tautological

For purely logical assertions (type hierarchies, definitions), TVs are optional — the boolean layer handles them.

## Numeric Contradictions

The inference engine detects contradictions between numeric bounds automatically. When `(> X A)` and `(< X B)` are both asserted for the same entity, and `B ≤ A` (making the range impossible), the engine derives ⊥.

```MeTTa
;; Contradiction: nothing can be both > 60 and < 50
(> price 60)
(< price 50)
;; => ⊥ (because 50 ≤ 60)

;; No contradiction: > 60 and < 70 overlap (e.g., 65 satisfies both)
(> price 60)
(< price 70)
;; => no ⊥
```

Use MeTTa's built-in `<` and `>` operators directly — do **not** encode thresholds into predicate names (e.g., avoid `priceAbove60k`).

### Prediction market example

```MeTTa
;; Market estimates with truth values
(≞ (> btc-price 60000) (STV 0.7 0.6))   ; 70% chance BTC > 60k
(≞ (< btc-price 50000) (STV 0.3 0.4))   ; 30% chance BTC < 50k

;; The engine detects: these two positions are mathematically contradictory
;; (50000 ≤ 60000, so no value can satisfy both bounds)
;; => ⊥
```

## Reference: Inference Engine

Below is the full content of the MeTTa inference engine (`inference.metta`). This is the actual code that powers entailment and contradiction detection in the pipeline:

```MeTTa

;; Let the space &a denote our propositions.
;;
;; 1. The relation (=> A B) satisfies:
;;  "whenever C => A, then C => B" (transitivity)

!(bind! &a (new-space))

;; Peano numerals for depth-bounded proof search
(: Nat Type)
(: Z Nat)
(: S (-> Nat Nat))

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

;; 3. find-evidence-for: boolean wrapper around find-evidence-for-tv
;; Returns evidence only when strength > 0 (i.e. the proof is valid).
;; All proof logic lives in find-evidence-for-tv below.
(= (find-evidence-for $to-prove)
   (let (≞ $ev (STV $s $c)) (find-evidence-for-tv $to-prove)
     (if (> $s 0.0) $ev (empty))))


;; 4. adding propositions (universal quantification)
;; add-proposition creates universal rules: (white swan) means ALL swans are white
(= (add-proposition $x) (add-atom &a $x)) ; do we really need the base atom, or only its relations?
(= (add-proposition ($attr $ob)) (add-atom &a (=> ($ob $x) ($attr $x))))

;; 5. adding existential facts (no universal rule)
;; add-existential only asserts a fact about a specific witness
;; use for "some X are Y" — does NOT entail "all X are Y"
(= (add-existential ($attr $witness)) (add-atom &a ($attr $witness)))


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

(= (is-a $x $y) ($y $x))

(= (add-proposition ($rel $ob $sub)) (add-atom &a (=> ($x $ob) ($rel $x $sub))))
(= (add-proposition ($rel $ob $sub)) (add-atom &a (=> ($x $sub) ($rel $ob $x))))

;; === PLN Truth Values ===
;; (≞ Proposition (STV strength confidence))
;;   strength   ∈ [0.0, 1.0] — probability
;;   confidence ∈ [0.0, 1.0] — weight of evidence

;; add-proposition-tv: adds proposition (with transitive rules) and attaches a truth value
(= (add-proposition-tv $prop (STV $s $c))
   (let () (add-proposition $prop)
     (let () (add-atom &a (≞ $prop (STV $s $c)))
       (≞ $prop (STV $s $c)))))

;; get-tv: retrieve the truth value for a proposition
(= (get-tv $expr) (match &a (≞ $expr (STV $s $c)) (STV $s $c)))

;; get-tv for derived transitive rules: (=> ($ob $x) ($attr $x)) inherits TV from ($attr $ob)
(= (get-tv (=> ($ob $x) ($attr $x)))
   (match &a (≞ ($attr $ob) (STV $s $c)) (STV $s $c)))

;; get-tv-or-default: returns TV if attached, (STV 1.0 1.0) if not
;; The default represents "no opinion" — it does not dilute or inflate
;; because 1.0 is the identity element for multiplication in combine-tv.
(= (get-tv-or-default $expr)
   (case (get-tv $expr)
     (((STV $s $c) (STV $s $c))
      (Empty (STV 1.0 1.0)))))

;; combine-tv: PLN deduction — s = s1 * s2, c = min(c1, c2)
(= (combine-tv (STV $s1 $c1) (STV $s2 $c2))
   (STV (* $s1 $s2) (if (< $c1 $c2) $c1 $c2)))

;; === find-evidence-for-tv: proof search with TV propagation ===
;; Mirrors find-evidence-for but returns (≞ evidence (STV s c))

;; Base: direct match, look up TV
(= (find-evidence-for-tv $to-prove $d)
   (let $ev (match &a $to-prove $to-prove)
     (let $tv (get-tv-or-default $to-prove)
       (≞ $ev $tv))))

;; Recursive: one transitive step, combine hypothesis TV with rule TV
(= (find-evidence-for-tv $to-prove (S $k))
   (let $hypothesis (match &a (=> $x $to-prove) $x)
     (let (≞ $ev $tv-hyp) (find-evidence-for-tv $hypothesis $k)
       (let $tv-rule (get-tv-or-default (=> $hypothesis $to-prove))
         (≞ (=> $ev $to-prove) (combine-tv $tv-hyp $tv-rule))))))

;; Conjunction: combine TVs of both branches
(= (find-evidence-for-tv (, $x $y) $d)
   (let (≞ $ex $tvx) (find-evidence-for-tv $x $d)
     (let (≞ $ey $tvy) (find-evidence-for-tv $y $d)
       (≞ (, $ex $ey) (combine-tv $tvx $tvy)))))

;; Disjunction introduction: (∨ A B) holds if either A or B holds
(= (find-evidence-for-tv (∨ $a $b) $d) (find-evidence-for-tv $a $d))
(= (find-evidence-for-tv (∨ $a $b) $d) (find-evidence-for-tv $b $d))

;; Disjunction elimination via classical equivalence: A ∨ B ≡ ¬A → B ≡ ¬B → A
;; add-proposition (∨ A B) adds the ∨ plus both implication directions,
;; enabling the recursive clause to derive either branch via is-not.
(= (add-proposition (∨ $a $b))
   (let () (add-atom &a (∨ $a $b))
     (let () (add-atom &a (=> (is-not $a) $b))
       (add-atom &a (=> (is-not $b) $a)))))

;; Convenience wrapper: default depth of 3
(= (find-evidence-for-tv $to-prove)
   (find-evidence-for-tv $to-prove (S (S (S Z)))))

;; === Numeric contradiction ===
;; Implication rules in the space + computed get-tv guards.
;; find-evidence-for-tv uses these generically via the recursive clause.

;; (> X A) ∧ (< X B) where B ≤ A → ⊥
!(add-atom &a (=> (, (> $x $a) (< $x $b)) ⊥))
(= (get-tv (=> (, (> $x $a) (< $x $b)) ⊥))
   (if (<= $b $a) (STV 1.0 1.0) (STV 0.0 0.0)))

;; (> X A) ∧ (<= X B) where B ≤ A → ⊥
!(add-atom &a (=> (, (> $x $a) (<= $x $b)) ⊥))
(= (get-tv (=> (, (> $x $a) (<= $x $b)) ⊥))
   (if (<= $b $a) (STV 1.0 1.0) (STV 0.0 0.0)))

;; (>= X A) ∧ (< X B) where B ≤ A → ⊥
!(add-atom &a (=> (, (>= $x $a) (< $x $b)) ⊥))
(= (get-tv (=> (, (>= $x $a) (< $x $b)) ⊥))
   (if (<= $b $a) (STV 1.0 1.0) (STV 0.0 0.0)))

;; (>= X A) ∧ (<= X B) where B < A → ⊥  (strict: at B = A, X = A satisfies both)
!(add-atom &a (=> (, (>= $x $a) (<= $x $b)) ⊥))
(= (get-tv (=> (, (>= $x $a) (<= $x $b)) ⊥))
   (if (< $b $a) (STV 1.0 1.0) (STV 0.0 0.0)))

;; === Negation bridge for numeric comparisons ===
;; Connects < / > / <= / >= facts to is-not of the opposite comparison,
;; enabling ∨-elimination via the recursive clause.

;; (< X B) → (is-not (> X A)) when B ≤ A
!(add-atom &a (=> (< $x $b) (is-not (> $x $a))))
(= (get-tv (=> (< $x $b) (is-not (> $x $a))))
   (if (<= $b $a) (STV 1.0 1.0) (STV 0.0 0.0)))

;; (> X A) → (is-not (< X B)) when B ≤ A
!(add-atom &a (=> (> $x $a) (is-not (< $x $b))))
(= (get-tv (=> (> $x $a) (is-not (< $x $b))))
   (if (<= $b $a) (STV 1.0 1.0) (STV 0.0 0.0)))

;; (<= X B) → (is-not (>= X A)) when B < A
!(add-atom &a (=> (<= $x $b) (is-not (>= $x $a))))
(= (get-tv (=> (<= $x $b) (is-not (>= $x $a))))
   (if (< $b $a) (STV 1.0 1.0) (STV 0.0 0.0)))

;; (>= X A) → (is-not (<= X B)) when B < A
!(add-atom &a (=> (>= $x $a) (is-not (<= $x $b))))
(= (get-tv (=> (>= $x $a) (is-not (<= $x $b))))
   (if (< $b $a) (STV 1.0 1.0) (STV 0.0 0.0)))

;; (<= X B) → (is-not (> X A)) when B ≤ A
!(add-atom &a (=> (<= $x $b) (is-not (> $x $a))))
(= (get-tv (=> (<= $x $b) (is-not (> $x $a))))
   (if (<= $b $a) (STV 1.0 1.0) (STV 0.0 0.0)))

;; (>= X A) → (is-not (< X B)) when B ≤ A
!(add-atom &a (=> (>= $x $a) (is-not (< $x $b))))
(= (get-tv (=> (>= $x $a) (is-not (< $x $b))))
   (if (<= $b $a) (STV 1.0 1.0) (STV 0.0 0.0)))

;; === Complement TVs ===
;; < is the negation of >=, > is the negation of <=

(= (get-tv (>= $x $a))
   (let (STV $s $c) (match &a (≞ (< $x $a) (STV $s $c)) (STV $s $c))
     (STV (- 1.0 $s) $c)))

(= (get-tv (< $x $a))
   (let (STV $s $c) (match &a (≞ (>= $x $a) (STV $s $c)) (STV $s $c))
     (STV (- 1.0 $s) $c)))

(= (get-tv (<= $x $a))
   (let (STV $s $c) (match &a (≞ (> $x $a) (STV $s $c)) (STV $s $c))
     (STV (- 1.0 $s) $c)))

(= (get-tv (> $x $a))
   (let (STV $s $c) (match &a (≞ (<= $x $a) (STV $s $c)) (STV $s $c))
     (STV (- 1.0 $s) $c)))
```

When generating a MeTTa expression, always wrap your final result in a single MeTTa code block: ```MeTTa\n```.
