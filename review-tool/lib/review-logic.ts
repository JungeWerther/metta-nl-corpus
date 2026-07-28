// Pure, DB-free logic shared by the server actions — unit-testable in isolation.
import { z } from "zod";

// ---------------------------------------------------------------------------
// Position / navigation
// ---------------------------------------------------------------------------

/** Clamp a requested position into [0, total-1]. NaN/negative -> 0; past-end
 * (incl. +Infinity) -> total-1; total<=0 -> 0. */
export function clampPosition(pos: number, total: number): number {
  if (total <= 0) return 0;
  if (Number.isNaN(pos) || pos < 0) return 0;
  if (pos > total - 1) return total - 1;
  return Math.floor(pos);
}

// ---------------------------------------------------------------------------
// Verdict derivation
// ---------------------------------------------------------------------------

export type Verdict = { premiseOk: boolean; hypothesisOk: boolean; note: string | null };

/** A row in Correct means both sides were accepted (carrying its note). */
export function verdictFromCorrect(note: string | null = null): Verdict {
  return { premiseOk: true, hypothesisOk: true, note };
}

/** Reconstruct the reviewer's choice from a Failed row's flags. */
export function verdictFromFailed(f: {
  premiseFailed: boolean;
  hypothesisFailed: boolean;
  note: string | null;
}): Verdict {
  return { premiseOk: !f.premiseFailed, hypothesisOk: !f.hypothesisFailed, note: f.note };
}

/** Both sides Yes => the pair is Correct; otherwise Failed. */
export function isCorrect(premiseOk: boolean, hypothesisOk: boolean): boolean {
  return premiseOk && hypothesisOk;
}

/** Trim a note; empty/whitespace/undefined becomes null. */
export function normalizeNote(note: string | null | undefined): string | null {
  if (note == null) return null;
  const t = note.trim();
  return t.length ? t : null;
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

export type RawCounts = {
  total: number;
  correct: number;
  failed: number;
  premiseFailed: number;
  hypothesisFailed: number;
  fixed: number;
  gold: number;
  goldFixed: number;
};

export type Stats = {
  total: number;
  reviewed: number;
  correct: number;
  failed: number;
  fixed: number;
  toFix: number;
  gold: number;
  goldDirect: number;
  goldFixed: number;
  premiseYes: number;
  premiseNo: number;
  hypothesisYes: number;
  hypothesisNo: number;
};

/** Derive display stats from raw DB counts. Safe when reviewed = 0. */
export function computeStats(r: RawCounts): Stats {
  const reviewed = r.correct + r.failed;
  return {
    total: r.total,
    reviewed,
    correct: r.correct,
    failed: r.failed,
    fixed: r.fixed,
    toFix: r.failed - r.fixed,
    gold: r.gold,
    goldDirect: r.gold - r.goldFixed,
    goldFixed: r.goldFixed,
    premiseYes: reviewed - r.premiseFailed,
    premiseNo: r.premiseFailed,
    hypothesisYes: reviewed - r.hypothesisFailed,
    hypothesisNo: r.hypothesisFailed,
  };
}

// ---------------------------------------------------------------------------
// Input validation (server-action boundary hardening)
// ---------------------------------------------------------------------------

const ID = z.string().trim().min(1).max(200);
const METTA = z.string().min(1).max(20_000);

export const verdictInput = z.object({
  annotationId: ID,
  premiseOk: z.boolean(),
  hypothesisOk: z.boolean(),
  note: z.string().max(4000).default(""),
});

export const fixInput = z.object({
  failedId: ID,
  fixedMettaPremise: METTA,
  fixedMettaHypothesis: METTA,
  note: z.string().max(4000).default(""),
});

export const failedIdInput = z.object({ failedId: ID });

export const positionInput = z.number().int().finite().optional();

export type VerdictInput = z.infer<typeof verdictInput>;
export type FixInput = z.infer<typeof fixInput>;
