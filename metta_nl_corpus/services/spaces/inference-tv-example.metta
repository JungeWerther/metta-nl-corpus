;; === PLN Truth Values: Worked Example ===
;; Demonstrates truth values and numeric contradiction detection.

!(import! &self inference)

;; === Existing Boolean inference still works ===
!(add-proposition (white swan))
!(add-proposition (swan this-swan))
!(find-evidence-for (white this-swan))
;; Expected: derives (white this-swan)

;; === Propositions with truth values ===
;; "Dogs shed fur" — high confidence from veterinary science
!(add-proposition-tv (shedsFur Dog) (STV 0.97 0.95))

;; "This is a dog" — observed directly
!(add-proposition-tv (is-a a-dog Dog) (STV 1.0 0.99))

;; === Query truth values ===
!(get-tv (shedsFur Dog))
;; Expected: (STV 0.97 0.95)

;; === Combine truth values (PLN deduction) ===
!(combine-tv (STV 0.9 0.8) (STV 0.7 0.6))
;; Expected: (STV 0.63 0.6) — s = 0.9*0.7, c = min(0.8, 0.6)

;; === Prediction Market Scenario ===
;; "BTC price above 60k by May 1st" — market estimate
!(add-proposition-tv (> btc-price 60000) (STV 0.7 0.6))

;; "BTC price below 50k by May 1st" — contrarian estimate
!(add-proposition-tv (< btc-price 50000) (STV 0.3 0.4))

;; Numeric contradiction: 50000 ≤ 60000, so (> X 60000) ∧ (< X 50000) → ⊥
!(find-evidence-for ⊥)
;; Expected: ⊥

;; === Non-contradictory overlapping ranges ===
;; (> btc-price 60000) and (< btc-price 70000) can both hold — no contradiction
;; If we had only these two, find-evidence-for ⊥ would return empty.
