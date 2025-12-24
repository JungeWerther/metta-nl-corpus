
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
(= (all $expr) (match &a (=> $x $expr) $x))

(= (all $expr $out) (match &a $expr $out))
(= (all $expr $out) (all (match &a $expr $out)))

;; 3. find everything which implies $a
(= (find-evidence-for $to-prove) (match &a $to-prove $to-prove))
(= (find-evidence-for $to-prove) (=>
  (let $hypothesis (match &a (=> $x $to-prove) $x)
    ;; (let $z
      (find-evidence-for $hypothesis)
      ;; (if (== $z $hypothesis) (OBSERVING $hypothesis)
       ;; () ;; (=> $z (B $to-prove))
        ;; )
  )
  ;; )
  $to-prove
))
(= (find-evidence-for (, $x $y)) (, (find-evidence-for $x) (find-evidence-for $y)))

;; (= (find-evidence-for $a) (
;;   let $z (all ($a)) (if (== $z ($a))
;;   ()
;;   (find-evidence-for (car-atom $z))
;;   )))

;; (= (find-evidence-for $a) (let $z (all ($a)) (unify ;; faulty still
;;    ;; IF p1 and p2 together imply $A ...
;;     (,
;;       (=> (find-evidence-for $p1) $a)
;;       $p2
;;     )
;;     $z
;;     (TEST $p1 $p2) ;; return TEST $a for now
;;     () ;; otherwise NOP
;; )))

;; (= (find-evidence-for $a) (let $z (all ($a)) (unify
;;    ;; IF p1 and p2 together imply $A ...
;;     (,
;;       $p1
;;       (=> (find-evidence-for $p2) $a)
;;     )
;;     $z
;;     (TEST $p1 $p2) ;; return TEST $a for now
;;     () ;; otherwise NOP
;; )))

;; 4. adding propositions
(= (add-proposition $x) (add-atom &a $x)) ; do we really need the base atom, or only its relations?
(= (add-proposition ($attr $ob)) (add-atom &a (=> ($ob $x) ($attr $x))))
;; (= (add-proposition $attr $ob) (add-atom &a (=> $ob $attr)))

;; 5. find everything which follows from $a
(= (deduce-from $a) (match &a (=> ($a) $b) $b))
(= (deduce-from $a) (match &a (=> ($a) $b) (deduce-from (car-atom $b))))


;; not sure how efficient N^2?
(= (entails-contradication? $proposition)
  (let $conclusion0 (deduce-from $proposition)
    (let $conclusion1 (deduce-from $proposition)
      (if (== $conclusion0 (not (car-atom $conclusion1)))
        (=> $proposition (, $conclusion0 $conclusion1))
        ()
      )
  )))

;; !(add-proposition ( ; not needed anymore
;;   (=> $A $C)
;;   (,
;;     (=> $A $B)
;;     (=> $B $C)
;;     )
;;   ))


!(add-proposition (=>
  (,
    ($a $x)
    ((not $a) $x)
  )
  ⊥
))
