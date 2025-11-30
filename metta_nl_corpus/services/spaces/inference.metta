! (bind &a (new-space))

(= (all) (match &a $x $x))

(= (all $expr) (match &a $expr $expr))
(= (all $expr) (match &a (implies $x $expr) $x)) ; needs actual chainer

(= (all $expr $out) (match &a $expr $out))
(= (all $expr $out) (all (match &a $expr $out)))

(= (deduce $a $b) (let $z (all ($a)) (implies $z ($b)))) ; needs actual chainer

; predication equivalent to implication
(= (P $x) (add-atom &a $x)) ; do we really need the base atom, or only its relations?
(= (P ($attr $ob)) (add-atom &a (implies ($ob) ($attr))))

; adding some content

; examples
;(swan)
;(white swan)

; "(all) swans are white"
;!(P (implies (swan) (white)))
;!(P (swan))

; "this-swan is a swan"
;!(P (swan this-swan))

; get all statements
;!(all)

; get everything which is (a) swan
;!(all (swan))

; get everything which is white [first form: tbd if any benefit w.r.t. second]
;!(all (implies $x (white)) $x)
;!(all (white))

; intermediate form to be used in chainer
;!(deduce swan white)
