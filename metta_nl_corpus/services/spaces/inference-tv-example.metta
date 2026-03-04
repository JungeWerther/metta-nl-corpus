;; === PLN Truth Values: Worked Example ===
;; Demonstrates that truth values coexist with the existing Boolean engine.

!(import! &self inference)

;; === Existing Boolean inference still works ===
!(add-proposition (white swan))
!(add-proposition (swan this-swan))
!(find-evidence-for (white this-swan))
;; Expected: derives (white this-swan) — no change from before

;; === New: propositions with truth values ===
;; "Dogs shed fur" — high confidence from veterinary science
!(add-proposition-tv (shedsFur Dog) (STV 0.97 0.95))

;; "This is a dog" — observed directly, very high confidence
!(add-proposition-tv (is-a a-dog Dog) (STV 1.0 0.99))

;; === Query truth values ===
!(get-tv (shedsFur Dog))
;; Expected: (STV 0.97 0.95)

!(get-tv (is-a a-dog Dog))
;; Expected: (STV 1.0 0.99)

;; === Boolean inference still works on TV-annotated facts ===
!(find-evidence-for (shedsFur a-dog))
;; Expected: derives (shedsFur a-dog) via transitivity

;; === TV-aware inference ===
!(find-evidence-for-tv (shedsFur Dog))
;; Expected: (≞ (shedsFur Dog) (STV 0.97 0.95))

;; === Combine truth values (PLN deduction) ===
!(combine-tv (STV 0.9 0.8) (STV 0.7 0.6))
;; Expected: (STV 0.63 0.6)  — s = 0.9*0.7, c = min(0.8, 0.6)

;; === Prediction Market Scenario ===
;; "BTC price above 60k" and "BTC price below 50k" are contrary predicates
!(add-contrary priceAbove60k priceBelow50k)

;; Market says: BTC above 60k with 70% confidence
!(add-proposition-tv (priceAbove60k BTC) (STV 0.7 0.6))

;; Another source says: BTC below 50k with 40% confidence
!(add-proposition-tv (priceBelow50k BTC) (STV 0.4 0.3))

;; This should derive ⊥ (contradiction)
!(find-evidence-for ⊥)
;; Expected: derives ⊥ — priceAbove60k and priceBelow50k are contrary

;; === Non-contradictory overlapping ranges ===
;; "above 60k" and "below 70k" can both be true — no contrary declared
;; !(add-proposition-tv (priceBelow70k BTC) (STV 0.8 0.5))
;; Would NOT produce ⊥ because no contraryAttribute relation exists
