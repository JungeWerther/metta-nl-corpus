
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

;; 3. find proofs for $to-prove (depth-bounded)
;; Base: direct match at any depth
(= (find-evidence-for $to-prove $d) (match &a $to-prove $to-prove))

;; Recursive: one step of transitive chaining (requires depth > 0)
(= (find-evidence-for $to-prove (S $k)) (=>
  (let $hypothesis (match &a (=> $x $to-prove) $x)
      (find-evidence-for $hypothesis $k)
  )
  $to-prove
))

;; Conjunction
(= (find-evidence-for (, $x $y) $d) (, (find-evidence-for $x $d) (find-evidence-for $y $d)))

;; Disjunction: (or A B) holds if either A or B holds
(= (find-evidence-for (or $a $b) $d) (find-evidence-for $a $d))
(= (find-evidence-for (or $a $b) $d) (find-evidence-for $b $d))

;; Convenience wrapper: default depth of 3
(= (find-evidence-for $to-prove) (find-evidence-for $to-prove (S (S (S Z)))))


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

(= (add-proposition ($rel $ob $sub)) (add-atom &a (=> ($x $ob) ($rel $x $sub))))
(= (add-proposition ($rel $ob $sub)) (add-atom &a (=> ($x $sub) ($rel $ob $x))))
