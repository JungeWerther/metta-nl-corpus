
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
