
; MeTTa functional logic

! (bind! &truth (new-space))
! (bind! &falsity (new-space))

; define identity
; =====================

(= (identity $t) $t)

; define true and false
; =====================

;; ( = (TruthValue) (-> $T $U (Either $T $U)))

;; (: true (TruthValue))
(= (true $a $b) $a)

;; (: false (TruthValue))
(= (false $a $b) $b)

; and, or, if
; to convince yourself, imagine this truth table:
; (_and)
;   F T = F T F = F
;   T F = T F T = F
; exercise: write down the truth table for 'or'
; =====================

(= (_and $p1 $p2) ($p1 $p2 $p1))
(= (_or $p1 $p2) ($p1 $p1 $p2))
(=
 ((when $when_true $when_false) $truth_value)
 ($truth_value $when_true $when_false))

; now we can define objects like 'swan' as observations where evaluating:
;   ((swan) true)
; yields the left side, which could be defined as all the propositions
; which are true of the object 'swan'
;
; similarly
;   ((swan) false)
; yields all the properties or observations of which we assert that they are
; false of the object 'swan'
; =====================
;; !((swan) true)

; (= (swan) (when
;            (Property white (Property bird ())) ; when true
;             (Property red-haired (Property fish ())) ; when false
;             ))

; storing such observations in a space, we need some helper functions to find
; all true and false properties of each object
; =====================
;; !(apply swan false)

(= (apply $name $function) (let $true
                             (($name) true) (let $false
                                              (($name) false) ($function $true $false))))

(=
 (flatten $key $space ())
 ())

(=
 (flatten $space (Property $const $list))
 (let $_ (add-atom $space ($const)) (Property $const (flatten $space $list))))

; to getting a flat representation of properties:
; bug: currently returning the space context twice
; =====================
; !(true_for swan)

(= (add_to $space $object $boolean) (flatten $space (($object) $boolean)))

(= (false_for $object) (let $y (add_to &falsity $object false) (match &falsity $x $x)))
(= (true_for $object) (let $y (add_to &truth $object true) (match &truth $x $x)))

; we have a contradiction when there is a property which exists in both
; the 'true' and the 'false' assertions of an object
; =====================
;; !(contradiction swan)

; (= (swan) (when
;            (Property white (Property red-haired ())) ; when true
;             (Property red-haired (Property bird ())) ; when false
;             ))

; ! (true_for swan)
; ! (false_for swan)

(= (contradiction $object) (unify &truth ($x) (let $y ($x) (unify &falsity $y $y Fail)) Fail))
; ! (contradiction swan)
