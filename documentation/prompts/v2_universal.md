# Metta-nl-corpus — v2 Universal (PLN-compatible)

This guideline extends v1_standard with one critical change: all expressions
must be compatible with the PeTTa PLN inference engine (`lib_pln.metta`).

---

## Key difference from v1: STV-annotated facts + consistent witnesses

### v1 (existential, inference-incompatible)
```metta
(dog some-dog)
(running some-dog)
```

### v2 (PLN-compatible)
```metta
(= (dog Dog1) (stv 1.0 0.9))
(= (running Dog1) (stv 1.0 0.9))
```

Every fact is wrapped in `(= ... (stv <strength> <confidence>))`.

---

## Witness naming convention

Use **capitalised numbered constants** for instances: `Dog1`, `Woman2`, `Man1`.
The same individual must use the **same name** across premise and hypothesis.

❌ **WRONG** (v1 style — different names for same entity):
```
premise:    (dog some-dog)
hypothesis: (dog a-dog)
```

✅ **CORRECT** (v2 style — consistent naming):
```
premise:    (= (dog Dog1) (stv 1.0 0.9))
hypothesis: (= (dog Dog1) (stv 1.0 0.9))
```

---

## General rules (use `=>` with variables)

When the text expresses a general truth, use implication:

```metta
!(=> (dog $x) (animal $x) (stv 0.95 0.8))
!(=> (running $x) (moving $x) (stv 1.0 0.9))
```

---

## STV values

- **Strength**: how true is this? `1.0` = certain, `0.5` = uncertain
- **Confidence**: how much evidence? Start at `0.9` for stated facts, `0.7` for inferred

---

## Negation

```metta
(= ((is-not running) Dog1) (stv 1.0 0.9))
```

---

## Full example

**Sentence**: "A bearded man is pulling on a rope."

```metta
(= (man Man1) (stv 1.0 0.9))
(= (bearded Man1) (stv 1.0 0.9))
(= (pulling-on Man1 Rope1) (stv 1.0 0.9))
(= (rope Rope1) (stv 1.0 0.9))
```

**Entailed hypothesis**: "A man is pulling something."

```metta
(= (man Man1) (stv 1.0 0.9))
(= (pulling-on Man1 Rope1) (stv 1.0 0.9))
```

The shared witness `Man1` makes the entailment immediately provable by the inference engine.

---

## Rules inherited from v1

All other rules from v1_standard apply: mandatory parentheses, predicate-first
order, compound predicates with hyphens, `is-not` for negation.
