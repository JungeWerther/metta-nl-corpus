!(import! &self inference)

;; examples

;; "(all) swans are white"
!(add-proposition (white swan))

; "this-swan is a swan / `swan` is a property of `this-swan`"
!(add-proposition (swan this-swan))

;; "my-vision is this-swan" -
;; ~is it a property of this swan that is in my vision?
;; or does `my field of vision` imply that there is a swan in there?
;; (by virtue of my observation)
!(add-proposition (this-swan in-my-vision))
!(add-proposition (in-my-vision I-have-eyes))

; get all statements
!(all)

; get everything which is (a) swan
;; !(all (swan))

; get everything which is white [first form: tbd if any benefit w.r.t. second]
;; !(all (=> $x (white)) $x)
;; !(all (white))

; "derive propositions from &a which imply white"
; i.e. 'find everything $z which is a swan' then deduce that '$z is white'
!(find-evidence-for (white I-have-eyes))
!(find-evidence-for (black swan))

;; !(deduce-from my-vision)
;; !(add-proposition (not white) (this-swan))
;; !(deduce-from this-swan)

;; !(all)

!(add-proposition (man Socrates))
!(add-proposition (mortal man))

!(find-evidence-for (mortal Socrates)) ;; returns [(=> (man Socrates) (mortal Socrates))]
;; !(find-evidence-for (=> a h)) ;; does not return evidence

!(add-proposition ((not mortal) Socrates))
!(find-evidence-for ⊥)

!(add-proposition ((not white) in-my-vision))
!(find-evidence-for ⊥)

;; --- Disjunction examples ---

;; "a person is running or walking" — at least one must hold
!(add-proposition (person a-person))
!(add-proposition (running a-person))

;; Provable: a-person is running, so (or running walking) holds
!(find-evidence-for (or (running a-person) (walking a-person)))

;; Not provable: neither swimming nor flying was asserted
!(find-evidence-for (or (swimming a-person) (flying a-person)))

;; --- Existential quantifier examples ---

;; "some dogs are brown" — witness-level fact, no universal rule
!(add-existential (dog some-dog))
!(add-existential (brown some-dog))

;; Provable: some-dog is brown (direct fact)
!(find-evidence-for (brown some-dog))

;; NOT provable: another-dog is a dog, but no universal "dogs are brown" rule exists
!(add-existential (dog another-dog))
!(find-evidence-for (brown another-dog))
