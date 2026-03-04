;; === PLN Truth Values: Worked Example ===
;; Demonstrates truth values and numeric contradiction detection.

!(import! &self inference)

;; === Existing Boolean inference still works ===
!(add-proposition (white swan))
!(add-proposition (swan this-swan))
!(find-evidence-for (white this-swan))
;; Expected: derives (white this-swan)

;; === Propositions with truth values ===
!(add-proposition-tv (shedsFur Dog) (STV 0.97 0.95))
!(add-proposition-tv (Dog a-dog) (STV 1.0 0.99))

;; === Query truth values ===
!(get-tv (shedsFur Dog))
;; Expected: (STV 0.97 0.95)

;; === Transitive inference with TV propagation ===
!(find-evidence-for-tv (shedsFur a-dog))
;; Expected: (≞ (=> (Dog a-dog) (shedsFur a-dog)) ...)

;; === Combine truth values (PLN deduction) ===
!(combine-tv (STV 0.9 0.8) (STV 0.7 0.6))
;; Expected: (STV 0.63 0.6) — s = 0.9*0.7, c = min(0.8, 0.6)

;; === Prediction Market Scenario ===
!(add-proposition-tv (> btc-price 60000) (STV 0.7 0.6))
!(add-proposition-tv (< btc-price 50000) (STV 0.3 0.4))

;; Boolean: detects contradiction
!(find-evidence-for ⊥)
;; Expected: ⊥

;; TV-aware: contradiction with combined truth value
!(find-evidence-for-tv ⊥)
;; Expected: (≞ ⊥ (STV 0.21 0.4))
;; s = 0.7 * 0.3 = 0.21, c = min(0.6, 0.4) = 0.4
;; The rule's computed TV is (STV 1.0 1.0) because 50000 ≤ 60000,
;; so the final TV is entirely determined by the proposition TVs.

;; === Non-contradictory ranges ===
;; If we had (> btc-price 60000) and (< btc-price 70000),
;; the rule's TV would be (STV 0.0 0.0) because 70000 > 60000,
;; zeroing out the result — no contradiction.
