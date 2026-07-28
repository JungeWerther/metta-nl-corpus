"use server";

import { prisma } from "@/lib/prisma";
import { getReviewer } from "@/lib/reviewer";
import { failedIdInput, fixInput, normalizeNote } from "@/lib/review-logic";
import type { FailedItem } from "./types";

/** List this reviewer's failed pairs (with their fix, if any). */
export async function getFailedList(): Promise<FailedItem[]> {
  const { id: rid } = await getReviewer();
  const rows = await prisma.failed.findMany({
    where: { reviewerId: rid },
    orderBy: { createdAt: "asc" },
    include: {
      fix: { select: { fixedMettaPremise: true, fixedMettaHypothesis: true, note: true } },
    },
  });
  return rows.map((r) => ({
    id: r.id,
    annotationId: r.annotationId,
    premise: r.premise,
    hypothesis: r.hypothesis,
    label: r.label,
    mettaPremise: r.mettaPremise,
    mettaHypothesis: r.mettaHypothesis,
    premiseFailed: r.premiseFailed,
    hypothesisFailed: r.hypothesisFailed,
    note: r.note,
    isFixed: r.isFixed,
    fixed: r.fix
      ? {
          fixedMettaPremise: r.fix.fixedMettaPremise,
          fixedMettaHypothesis: r.fix.fixedMettaHypothesis,
          note: r.fix.note,
        }
      : null,
  }));
}

/**
 * Fix a failed pair. Validated + atomic + idempotent. Writes a Fixed row
 * (original + fixed MeTTa, old->new UUID, optional note) and a Final(FIXED) row,
 * sets isFixed. Ownership-checked (can't fix another reviewer's row).
 */
export async function submitFix(
  failedId: string,
  fixedMettaPremise: string,
  fixedMettaHypothesis: string,
  note: string = "",
): Promise<{ ok: true } | { error: string }> {
  const { id: rid, name: rname } = await getReviewer();

  const parsed = fixInput.safeParse({ failedId, fixedMettaPremise, fixedMettaHypothesis, note });
  if (!parsed.success) return { error: "Both fixed MeTTa fields are required." };
  const input = parsed.data;

  const p = input.fixedMettaPremise.trim();
  const h = input.fixedMettaHypothesis.trim();
  if (!p || !h) return { error: "Both fixed MeTTa fields are required." };

  const failed = await prisma.failed.findUnique({ where: { id: input.failedId } });
  // Ownership check: never operate on another reviewer's Failed row (IDOR).
  if (!failed || failed.reviewerId !== rid) return { error: "Failed row not found." };

  const fixNote = normalizeNote(input.note);
  const stamp = { reviewerId: rid, reviewerName: rname };

  await prisma.$transaction([
    prisma.fixed.deleteMany({ where: { failedId: input.failedId } }),
    prisma.final.deleteMany({
      where: { sourceAnnotationId: failed.annotationId, reviewerId: rid, origin: "FIXED" },
    }),
    prisma.fixed.create({
      data: {
        failedId: input.failedId,
        oldUuid: failed.annotationId,
        premise: failed.premise,
        hypothesis: failed.hypothesis,
        label: failed.label,
        originalMettaPremise: failed.mettaPremise,
        originalMettaHypothesis: failed.mettaHypothesis,
        fixedMettaPremise: p,
        fixedMettaHypothesis: h,
        note: fixNote,
        ...stamp,
      },
    }),
    prisma.final.create({
      data: {
        sourceAnnotationId: failed.annotationId,
        premise: failed.premise,
        hypothesis: failed.hypothesis,
        label: failed.label,
        mettaPremise: p,
        mettaHypothesis: h,
        note: fixNote,
        origin: "FIXED",
        ...stamp,
      },
    }),
    prisma.failed.update({ where: { id: input.failedId }, data: { isFixed: true } }),
  ]);

  return { ok: true };
}

/** Undo a fix. Ownership-checked. */
export async function revertFix(
  failedId: string,
): Promise<{ ok: true } | { error: string }> {
  const { id: rid } = await getReviewer();

  const parsed = failedIdInput.safeParse({ failedId });
  if (!parsed.success) return { error: "Invalid input." };
  const id = parsed.data.failedId;

  const failed = await prisma.failed.findUnique({ where: { id } });
  // Ownership check: never operate on another reviewer's Failed row (IDOR).
  if (!failed || failed.reviewerId !== rid) return { error: "Failed row not found." };

  await prisma.$transaction([
    prisma.fixed.deleteMany({ where: { failedId: id } }),
    prisma.final.deleteMany({
      where: { sourceAnnotationId: failed.annotationId, reviewerId: rid, origin: "FIXED" },
    }),
    prisma.failed.update({ where: { id }, data: { isFixed: false } }),
  ]);

  return { ok: true };
}
