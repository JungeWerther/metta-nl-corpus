# Metta-nl-corpus
__Reference document for Natural language <> MeTTa experessions.__

This is a basic guideline outlining some principles for the conversion of natural language to s-expressions in MeTTa.

## Basics
Recall that an s-expression is any expression of the form `(x1 x2 ... xn)` where `x_i` can be replaced with an arbitrary value, which may itself be an s-expression. Any such expression is refered to here as an `Atom`.

In MeTTa, the keyword `:` has a special meaning, namely to assign a __type__ to some keyword. For example, considering that "socrates is an instance of a Human" I could write

```lisp
(: Socrates Human)
```

Let's say I wanted to make a basic assertion, or "predicate" something about an Atom. For example, imagine I wanted to say that "Socrates has black hair".
One way of expressing this in MeTTa would be so assign the property of "blackhairedness" to `Socrates`:

```lisp
; (Predicate Object)
(hasBlackHair Socrates)
```

Here we've invented a _unary_ (= taking one argument) predicate "hasBlackHair". You can think of it as a _function_ that takes some object, and produces either a new Atom, or a Boolean value, depending on how you look at it and what you want to do.
If we were to assign a type to "hasBlackHair", we could write it as follows:

```
; traditionally
(: hasBlackHair (-> Object Bool))

; in our convention
(: hasBlackHair (-> Object Atom))
```

But wait a second! What if we wanted to compare the hair colour of different humans. At the face of it, "blackHairedNess" has nothing to do with "blondHairedNess"!

Luckily, we can even make it more granular, by imagining a _binary_ predicate "hasHairColour" that takes an Object and a Colour, to produce the same meaning.

```
(: hasHairColour (-> Object Colour Atom))
(hasHairColour Socrates Black)
```

__Both expressions are valid!__

# Definite versus indefinite articles

In the case of Socrates, it's pretty obvious who we're talking about. Socrates can only have one meaning, because it's not only a _name_, and we all know who we're talking about. So two statements mentioning Socrates are probably talking about the same person.
Generally speaking though, it won't be possible to make such assignations.

Consider the sentence "The woman in white". How would we express this? There are no assertions being made here, but the sentence is certainly not devoid of meaning.
Furthermore, compare this with the sentence "A woman in white". The meaning of these two sentences could not be more different, especially when used within a _context_.

So, if you had the intuition to represent this concept like
```lisp
(: woman Human)
(dressedInWhite woman)
```

then this would almost certainly lead to problems down the line!

Another thing: when I'm talking about "__the__ woman" (definite case), I almost always need to refer to some _context_ if I were to want to answer the very natural question: "__which__ woman?".

This means that objects with a definite article are _reflexive_ in the sense that their _reference_ or _name_ should be infered from the _context_. Since the reference of such (reflexive) phrases should be determined after the fact, we opt to defer assignation of the referent to a name. We write:

```lisp
(the (: $x Woman) (dressedIn $x white))
```

Notice the `$` before `$x`. Here, this means that `$x` is a locally scoped variable. This will have advantages down the line,
but for the purposes of this project, do not worry about the implementation of `the`.

Some examples:

```lisp
; the cat jumped off the roof
(the (: $x Cat) (jumpedOff $x (the (: $y Roof) ())))

; some elephant in the room
(some (: $x Elephant) (isIn $x (the (: $y Room) ()))))

; I knew that John was angry
(knew I (felt John angry))

; it was a day to remember
(the (: $x Day) (wasMemorable $x))

; a blue wizard appeared suddenly
(appearedSuddenly (a (: $z Wizard) (hasColour $z blue)))
```

Some notes:
- "the" and "this" almost always carry the same meaning. Even they it'll be possible to use them interchangably in most cases, let's use them in the way they are mentioned.
- same for "a" and "some"
- let's agree to place the object before the attribute in binary predicates: `(hasColour John green)` over `(hasColour green John)`
- try to make predicates as granular as possible! `(isColour John yellow)` is more meaningful than `(isYellow John)`.
- there are multiple correct answers
- words that are implicitly reflexive, like 'I' or 'this' (when used as a proper noun) can be represented as-is (because they will get an explicit definition down the line)

## Quantification, negation, products

Great! What about sentences like "all swans are white" or "there exists a black swan"? We handle them just in the same way as before, but let's agree to use two special keywords `exists` and `forall` to represent existential and universal quantification, accordingly:

```lisp
; all swans are white
(forall (: $x Swan) (hasColour $x white))

; there exists a black swan
(exists (: $x Swan) (hasColour $x black))
```

Also, when we're dealing with a negation, let's represent it with the keyword `not`:

```lisp
; there is not a hair on my head that considers this
(not (exists (: $x Hair) (, (isOn $x myHead) (considers $x this))))

; it's also fine to present a synonym, when dealing with expressions
(stronglyDisconsider I this)
```

Hey! what is the `,` doing there? Well, in the Curry-Howard correspondence, the connective `and` in logic corresponds to the product-type! So it should be possible to use `,` and `and` interchangeably.

```lisp
; Kayley's head is blue and red
(, (hasColour (headOf Kayley) blue) (hasColour (headOf Kayley) red))

; (equivalent)
(and (hasBlueHead Kayley) (hasRedHead Kayley))

; (equivalent, better)
(the (: $x Human) (
  and (hasName $x Kayley) (
  and (hasColour (headOf $x) blue)
      (hasColour (headOf $x) red)
  )))
```

Some rules:
- Let's agree that `and` and `or` always take two arguments. Use nesting (like a `List` datastructure) if you need to chain connectives.
- `,` can be used instead of `and`, while `+` is equivalent to `or`. Let's prefer to use `and` and `or` for readibility.
