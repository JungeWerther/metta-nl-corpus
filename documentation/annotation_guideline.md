# Metta-nl-corpus
__Reference document for Natural language <> MeTTa experessions.__

## Basic expressions


```lisp
; John has a green hat
```

```lisp
(Predicate Object)
; or
(hasBlueHair Bob)
```

In case of a binary predicate like "Robin loves James", we can write

```lisp
(Predicate Subject Object)
; or
(loves Robin James)
```

; ---------------------------------------------------------


(A B (C))

(: B (-> Atom Type))
(: A (-> $T (B $T)))

(= (A $x) (B $x))
!(A r) ; (B r)

; -------------------


(: $x Swan) ; 'some swan'

(forall (: $x Swan) (isWhite $x)) ; all swans are white
(exists (: $x Swan) (isBlack $x)) ; there is a black swan

(: isGreen (-> Atom Type)) ; or:
(: isGreen (-> Atom Bool))

(isGreen Bob) ; bob is green



(: sittingIn (-> Object Location Type))
(: sitting (-> Object (Maybe Location) Type))



; questions
(match &self (isGreen $x) $x)


















; bob lives in a green house
(livesInGreenHouse Bob)

(: Colour (-> Atom Type))

;
(property Bob (hasColour Green))
(hasColour)

; bob lives
(lives Bob)

; bob won the lotery
(won Bob Lotery)

(won bob (postcode_lottery_us_12 Lottery)
