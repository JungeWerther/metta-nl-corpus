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
