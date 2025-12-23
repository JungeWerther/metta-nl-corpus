# Metta-nl-corpus
__Reference document for Natural language <> MeTTa experessions.__

This is a basic guideline outlining some principles for the conversion of natural language to s-expressions in MeTTa.

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
(blue wizard)
(suddenlyAppeared wizard)
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

Also, when we're dealing with a negation, let's represent it with the keyword `not`:

```MeTTa
; there is not a hair on my head that considers this
(hair hair-on-my-head) ; hair-on-my-head is a hair
(on-my-head hair-on-my-head) ; hair-on-my-head is on my head
(not (considers-this hair-on-my-head))

; it's also fine to present a synonym, when dealing with expressions
(not-considered-by-me this)
```

```MeTTa
; Kayley's head is blue and red

(woman kayley)
(blue-head kayley)
(red-head kayley)

; equivalent:
(, (blue-head kayley) (red-head kayley))
```

When generating a MeTTa expression, always wrap your final result in a single MeTTa code block: ```MeTTa\n```.
