import { describe, expect, it } from "vitest";
import {
  clampPosition,
  computeStats,
  failedIdInput,
  fixInput,
  isCorrect,
  normalizeNote,
  verdictFromCorrect,
  verdictFromFailed,
  verdictInput,
} from "@/lib/review-logic";

describe("clampPosition", () => {
  it("clamps into [0, total-1]", () => {
    expect(clampPosition(-5, 10)).toBe(0);
    expect(clampPosition(0, 10)).toBe(0);
    expect(clampPosition(9, 10)).toBe(9);
    expect(clampPosition(100, 10)).toBe(9);
    expect(clampPosition(3, 10)).toBe(3);
  });
  it("handles empty totals, NaN, Infinity, fractional", () => {
    expect(clampPosition(5, 0)).toBe(0);
    expect(clampPosition(5, -3)).toBe(0);
    expect(clampPosition(Number.NaN, 10)).toBe(0);
    expect(clampPosition(Number.POSITIVE_INFINITY, 10)).toBe(9);
    expect(clampPosition(3.9, 10)).toBe(3);
  });
});

describe("verdict derivation", () => {
  it("correct means both yes", () => {
    expect(verdictFromCorrect()).toEqual({ premiseOk: true, hypothesisOk: true, note: null });
  });
  it("failed flags invert to ok booleans", () => {
    expect(verdictFromFailed({ premiseFailed: true, hypothesisFailed: false, note: "x" })).toEqual({
      premiseOk: false,
      hypothesisOk: true,
      note: "x",
    });
    expect(verdictFromFailed({ premiseFailed: false, hypothesisFailed: true, note: null })).toEqual({
      premiseOk: true,
      hypothesisOk: false,
      note: null,
    });
    expect(verdictFromFailed({ premiseFailed: true, hypothesisFailed: true, note: null })).toEqual({
      premiseOk: false,
      hypothesisOk: false,
      note: null,
    });
  });
  it("isCorrect is true only when both yes", () => {
    expect(isCorrect(true, true)).toBe(true);
    expect(isCorrect(true, false)).toBe(false);
    expect(isCorrect(false, true)).toBe(false);
    expect(isCorrect(false, false)).toBe(false);
  });
});

describe("normalizeNote", () => {
  it("trims and nullifies empties", () => {
    expect(normalizeNote(null)).toBeNull();
    expect(normalizeNote(undefined)).toBeNull();
    expect(normalizeNote("")).toBeNull();
    expect(normalizeNote("   ")).toBeNull();
    expect(normalizeNote("  hi ")).toBe("hi");
  });
});

describe("computeStats", () => {
  it("derives every field", () => {
    const s = computeStats({
      total: 100,
      correct: 30,
      failed: 20,
      premiseFailed: 8,
      hypothesisFailed: 15,
      fixed: 5,
      gold: 35,
      goldFixed: 5,
    });
    expect(s).toEqual({
      total: 100,
      reviewed: 50,
      correct: 30,
      failed: 20,
      fixed: 5,
      toFix: 15,
      gold: 35,
      goldDirect: 30,
      goldFixed: 5,
      premiseYes: 42,
      premiseNo: 8,
      hypothesisYes: 35,
      hypothesisNo: 15,
    });
  });
  it("is safe when nothing reviewed (no negatives / NaN)", () => {
    const s = computeStats({
      total: 10,
      correct: 0,
      failed: 0,
      premiseFailed: 0,
      hypothesisFailed: 0,
      fixed: 0,
      gold: 0,
      goldFixed: 0,
    });
    expect(s.reviewed).toBe(0);
    expect(s.premiseYes).toBe(0);
    expect(s.hypothesisYes).toBe(0);
    expect(s.toFix).toBe(0);
    expect(s.goldDirect).toBe(0);
  });
});

describe("verdictInput validation", () => {
  it("accepts valid input", () => {
    expect(
      verdictInput.safeParse({ annotationId: "abc", premiseOk: true, hypothesisOk: false, note: "n" })
        .success,
    ).toBe(true);
  });
  it("defaults note to empty string", () => {
    const r = verdictInput.safeParse({ annotationId: "a", premiseOk: true, hypothesisOk: true });
    expect(r.success).toBe(true);
    if (r.success) expect(r.data.note).toBe("");
  });
  it("rejects empty id, non-boolean flags, and oversized note", () => {
    expect(verdictInput.safeParse({ annotationId: "", premiseOk: true, hypothesisOk: true }).success).toBe(false);
    expect(
      verdictInput.safeParse({ annotationId: "a", premiseOk: "yes", hypothesisOk: true }).success,
    ).toBe(false);
    expect(
      verdictInput.safeParse({ annotationId: "a", premiseOk: true, hypothesisOk: true, note: "x".repeat(5000) })
        .success,
    ).toBe(false);
  });
});

describe("fixInput / failedIdInput validation", () => {
  it("fixInput requires id + non-empty metta", () => {
    expect(
      fixInput.safeParse({ failedId: "f", fixedMettaPremise: "(a b)", fixedMettaHypothesis: "(c d)" }).success,
    ).toBe(true);
    expect(
      fixInput.safeParse({ failedId: "f", fixedMettaPremise: "", fixedMettaHypothesis: "(c d)" }).success,
    ).toBe(false);
    expect(
      fixInput.safeParse({ failedId: "", fixedMettaPremise: "(a b)", fixedMettaHypothesis: "(c d)" }).success,
    ).toBe(false);
  });
  it("failedIdInput validates the id", () => {
    expect(failedIdInput.safeParse({ failedId: "x" }).success).toBe(true);
    expect(failedIdInput.safeParse({ failedId: "" }).success).toBe(false);
  });
});
