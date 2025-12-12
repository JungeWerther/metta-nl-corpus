!(import! &self inference)

;; examples

;; "(all) swans are white"
!(add-proposition (=> (swan) (white)))

; "this-swan is a swan / `swan` is a property of `this-swan`"
!(add-proposition (swan this-swan))

;; "my-vision is this-swan" -
;; ~is it a property of this swan that is in my vision?
;; or does `my field of vision` imply that there is a swan in there?
;; (by virtue of my observation)
!(add-proposition (this-swan my-vision))
!(add-proposition (my-vision I-have-eyes))

; get all statements
!(all)

; get everything which is (a) swan
!(all (swan))

; get everything which is white [first form: tbd if any benefit w.r.t. second]
!(all (=> $x (white)) $x)
!(all (white))

; "derive propositions from &a which imply white"
; i.e. 'find everything $z which is a swan' then deduce that '$z is white'
!(find-evidence-for white)

!(deduce-from my-vision)
!(add-proposition (~ white) (this-swan))
!(deduce-from this-swan)

!(entails-contradication? this-swan)
!(entails-contradication? swan)

;; questions:
;; - naming conventions? capital vs lowercase?
