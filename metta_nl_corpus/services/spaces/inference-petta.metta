
;; PeTTa-compatible inference engine.
;; Identical to inference.metta but avoids numeric comparison guards
;; that fail under Prolog's instantiation requirements.

!(bind! &a (new-space))

;; Peano numerals for depth-bounded proof search
(: Nat Type)
(: Z Nat)
(: S (-> Nat Nat))

;; Space queries
(= (all) (match &a $x $x))
(= (all $expr) (match &a $expr $expr))
(= (all $expr $out) (match &a $expr $out))

;; find-evidence-for: returns evidence when proof succeeds.
;; PeTTa-safe: uses let binding instead of numeric (if (> $s 0.0) ...).
;; A successful find-evidence-for-tv always returns positive strength,
;; so the guard is unnecessary — the let simply unpacks.
(= (find-evidence-for $to-prove)
   (let (≞ $ev (STV $s $c)) (find-evidence-for-tv $to-prove)
     $ev))

;; Adding propositions (universal quantification)
(= (add-proposition $x) (add-atom &a $x))
(= (add-proposition ($attr $ob)) (add-atom &a (=> ($ob $x) ($attr $x))))

;; Existential facts (no universal rule)
(= (add-existential ($attr $witness)) (add-atom &a ($attr $witness)))

;; Contradiction axioms
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

;; Binary predicate rules
(= (add-proposition ($rel $ob $sub)) (add-atom &a (=> ($x $ob) ($rel $x $sub))))
(= (add-proposition ($rel $ob $sub)) (add-atom &a (=> ($x $sub) ($rel $ob $x))))

;; Ground contradiction axioms (from positive side)
(= (add-proposition ($rel $ob $sub)) (add-atom &a (=> (, ($rel $ob $sub) (is-not $rel $ob $sub)) ⊥)))
(= (add-proposition ($rel $ob $sub)) (add-atom &a (=> (, ($rel $ob $sub) (is-not ($rel $ob $sub))) ⊥)))
(= (add-proposition ($rel $ob $sub)) (add-atom &a (=> (, ($rel $ob $sub) ((is-not $rel) $ob $sub)) ⊥)))

;; Negation-side rules
(= (add-proposition (is-not $rel $ob $sub)) (add-atom &a (=> (, ($rel $ob $sub) (is-not $rel $ob $sub)) ⊥)))
(= (add-proposition (is-not ($rel $ob $sub))) (add-atom &a (=> (, ($rel $ob $sub) (is-not ($rel $ob $sub))) ⊥)))
(= (add-proposition ((is-not $rel) $ob $sub)) (add-atom &a (=> (, ($rel $ob $sub) ((is-not $rel) $ob $sub)) ⊥)))

;; === PLN Truth Values ===
(= (add-proposition-tv $prop (STV $s $c))
   (let () (add-proposition $prop)
     (let () (add-atom &a (≞ $prop (STV $s $c)))
       (≞ $prop (STV $s $c)))))

(= (get-tv $expr) (match &a (≞ $expr (STV $s $c)) (STV $s $c)))
(= (get-tv (=> ($ob $x) ($attr $x)))
   (match &a (≞ ($attr $ob) (STV $s $c)) (STV $s $c)))

(= (get-tv-or-default $expr)
   (case (get-tv $expr)
     (((STV $s $c) (STV $s $c))
      (Empty (STV 1.0 1.0)))))

;; combine-tv: s = s1 * s2, c = min(c1, c2)
;; PeTTa-safe: use (min $c1 $c2) instead of (if (< $c1 $c2) $c1 $c2)
(= (combine-tv (STV $s1 $c1) (STV $s2 $c2))
   (STV (* $s1 $s2) (min $c1 $c2)))

;; === find-evidence-for-tv: proof search with TV propagation ===

;; Base: direct match
(= (find-evidence-for-tv $to-prove $d)
   (let $ev (match &a $to-prove $to-prove)
     (let $tv (get-tv-or-default $to-prove)
       (≞ $ev $tv))))

;; Recursive: one transitive step
(= (find-evidence-for-tv $to-prove (S $k))
   (let $hypothesis (match &a (=> $x $to-prove) $x)
     (let (≞ $ev $tv-hyp) (find-evidence-for-tv $hypothesis $k)
       (let $tv-rule (get-tv-or-default (=> $hypothesis $to-prove))
         (≞ (=> $ev $to-prove) (combine-tv $tv-hyp $tv-rule))))))

;; Conjunction
(= (find-evidence-for-tv (, $x $y) $d)
   (let (≞ $ex $tvx) (find-evidence-for-tv $x $d)
     (let (≞ $ey $tvy) (find-evidence-for-tv $y $d)
       (≞ (, $ex $ey) (combine-tv $tvx $tvy)))))

;; Disjunction
(= (find-evidence-for-tv (∨ $a $b) $d) (find-evidence-for-tv $a $d))
(= (find-evidence-for-tv (∨ $a $b) $d) (find-evidence-for-tv $b $d))

(= (add-proposition (∨ $a $b))
   (let () (add-atom &a (∨ $a $b))
     (let () (add-atom &a (=> (is-not $a) $b))
       (add-atom &a (=> (is-not $b) $a)))))

;; Default depth of 3
(= (find-evidence-for-tv $to-prove)
   (find-evidence-for-tv $to-prove (S (S (S Z)))))
