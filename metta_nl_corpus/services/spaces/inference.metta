
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

(= (is-a $x $y) ($y $x))

(= (add-proposition ($rel $ob $sub)) (add-atom &a (=> ($x $ob) ($rel $x $sub))))
(= (add-proposition ($rel $ob $sub)) (add-atom &a (=> ($x $sub) ($rel $ob $x))))

;; === PLN Truth Values ===
;; (≞ Proposition (STV strength confidence))
;;   strength   ∈ [0.0, 1.0] — probability
;;   confidence ∈ [0.0, 1.0] — weight of evidence

;; add-proposition-tv: adds proposition (with transitive rules) and attaches a truth value
(= (add-proposition-tv $prop (STV $s $c))
   (let () (add-proposition $prop)
     (let () (add-atom &a (≞ $prop (STV $s $c)))
       (≞ $prop (STV $s $c)))))

;; get-tv: retrieve the truth value for a proposition
(= (get-tv $expr) (match &a (≞ $expr (STV $s $c)) (STV $s $c)))

;; get-tv-or-default: returns TV if attached, otherwise (STV 1.0 1.0)
(= (get-tv-or-default $expr)
   (case (get-tv $expr)
     (((STV $s $c) (STV $s $c))
      (Empty (STV 1.0 1.0)))))

;; combine-tv: PLN deduction — s = s1 * s2, c = min(c1, c2)
(= (combine-tv (STV $s1 $c1) (STV $s2 $c2))
   (STV (* $s1 $s2) (if (< $c1 $c2) $c1 $c2)))

;; === find-evidence-for-tv: proof search with TV propagation ===
;; Mirrors find-evidence-for but returns (≞ evidence (STV s c))

;; Base: direct match, look up TV
(= (find-evidence-for-tv $to-prove $d)
   (let $ev (match &a $to-prove $to-prove)
     (let $tv (get-tv-or-default $to-prove)
       (≞ $ev $tv))))

;; Recursive: one transitive step, propagate hypothesis TV
(= (find-evidence-for-tv $to-prove (S $k))
   (let $hypothesis (match &a (=> $x $to-prove) $x)
     (let (≞ $ev $tv-hyp) (find-evidence-for-tv $hypothesis $k)
       (≞ (=> $ev $to-prove) $tv-hyp))))

;; Conjunction: combine TVs of both branches
(= (find-evidence-for-tv (, $x $y) $d)
   (let (≞ $ex $tvx) (find-evidence-for-tv $x $d)
     (let (≞ $ey $tvy) (find-evidence-for-tv $y $d)
       (≞ (, $ex $ey) (combine-tv $tvx $tvy)))))

;; Convenience wrapper: default depth of 3
(= (find-evidence-for-tv $to-prove)
   (find-evidence-for-tv $to-prove (S (S (S Z)))))

;; === Numeric contradiction ===
;; (> X A) ∧ (< X B) where B ≤ A → ⊥
(= (find-evidence-for ⊥ $d)
   (let (> $x $a) (match &a (> $x $a) (> $x $a))
     (let (< $x $b) (match &a (< $x $b) (< $x $b))
       (if (<= $b $a) ⊥ (empty)))))
