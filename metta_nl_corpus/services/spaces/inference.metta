
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
(= (find-evidence-for $a) (all $a))

(= (find-evidence-for $a) (
  let $z (all ($a)) (if (== $z ($a))
  ()
  (find-evidence-for (car-atom $z))
  )))

;; 4. adding propositions
(= (add-proposition $x) (add-atom &a $x)) ; do we really need the base atom, or only its relations?
(= (add-proposition ($attr $ob)) (add-atom &a (=> ($ob) ($attr))))
(= (add-proposition $attr $ob) (add-atom &a (=> $ob $attr)))

;; 5. find everything which follows from $a
(= (deduce-from $a) (match &a (=> ($a) $b) $b))
(= (deduce-from $a) (match &a (=> ($a) $b) (deduce-from (car-atom $b))))


;; not sure how efficient N^2?
(= (entails-contradication? $proposition)
  (let $conclusion0 (deduce-from $proposition)
    (let $conclusion1 (deduce-from $proposition)
      (if (== $conclusion0 (~ (car-atom $conclusion1)))
        (=> $proposition (, $conclusion0 $conclusion1))
        ()
      )
  )))
