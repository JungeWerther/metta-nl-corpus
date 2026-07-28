"use server";

import { prisma } from "@/lib/prisma";
import { getReviewer } from "@/lib/reviewer";
import {
  clampPosition,
  isCorrect,
  normalizeNote,
  verdictFromCorrect,
  verdictFromFailed,
  verdictInput,
} from "@/lib/review-logic";
import type { ReviewData } from "./types";

const ORDER = [{ sourceIndex: "asc" as const }, { id: "asc" as const }];

/** Load the annotation at `position` (defaults to the first unreviewed = resume). */
export async function getReviewData(position?: number): Promise<ReviewData> {
  const { id: rid } = await getReviewer();

  // Load the ordered id list plus this reviewer's verdicts so we can locate the
  // first genuinely UNREVIEWED pair. Resuming by count (skip = reviewed) is only
  // correct when reviewing is strictly sequential; once a pair is skipped it
  // silently strands that pair. Deriving position from actual verdicts is O(total)
  // over ids only (cheap for this dataset) and is always correct.
  const [ordered, correctIds, failedIds] = await Promise.all([
    prisma.annotation.findMany({ orderBy: ORDER, select: { id: true } }),
    prisma.correct.findMany({ where: { reviewerId: rid }, select: { annotationId: true } }),
    prisma.failed.findMany({ where: { reviewerId: rid }, select: { annotationId: true } }),
  ]);

  const total = ordered.length;
  const reviewedIds = new Set<string>();
  correctIds.forEach((r) => reviewedIds.add(r.annotationId));
  failedIds.forEach((r) => reviewedIds.add(r.annotationId));
  const correct = correctIds.length;
  const failed = failedIds.length;
  const reviewed = correct + failed;

  let firstUnreviewed: number | null = null;
  for (let i = 0; i < total; i++) {
    if (!reviewedIds.has(ordered[i].id)) {
      firstUnreviewed = i;
      break;
    }
  }

  const progress = { reviewed, correct, failed, total, firstUnreviewed };

  if (total === 0) {
    return { position: 0, total, annotation: null, verdict: null, progress };
  }

  // Explicit position (navigation) is honored; a bare reload resumes at the first
  // unreviewed pair, or the last pair when everything is done.
  const pos = clampPosition(position ?? firstUnreviewed ?? total - 1, total);

  const [annotation] = await prisma.annotation.findMany({
    orderBy: ORDER,
    skip: pos,
    take: 1,
    select: {
      id: true,
      premise: true,
      hypothesis: true,
      label: true,
      mettaPremise: true,
      mettaHypothesis: true,
      sourceIndex: true,
      generationModel: true,
    },
  });

  let verdict: ReviewData["verdict"] = null;
  if (annotation) {
    const c = await prisma.correct.findFirst({
      where: { annotationId: annotation.id, reviewerId: rid },
      select: { note: true },
    });
    if (c) {
      verdict = verdictFromCorrect(c.note);
    } else {
      const f = await prisma.failed.findFirst({
        where: { annotationId: annotation.id, reviewerId: rid },
        select: { premiseFailed: true, hypothesisFailed: true, note: true },
      });
      if (f) verdict = verdictFromFailed(f);
    }
  }

  return { position: pos, total, annotation: annotation ?? null, verdict, progress };
}

/**
 * Record (or change) a verdict for one annotation, scoped to this reviewer.
 * Validated input; atomic cascade (removes only THIS pair's rows for THIS
 * reviewer, then writes the new verdict). Stamps reviewer id + name.
 */
export async function submitVerdict(
  annotationId: string,
  premiseOk: boolean,
  hypothesisOk: boolean,
  note: string,
): Promise<{ ok: true } | { error: string }> {
  const { id: rid, name: rname } = await getReviewer();

  const parsed = verdictInput.safeParse({ annotationId, premiseOk, hypothesisOk, note });
  if (!parsed.success) return { error: "Invalid input." };
  const input = parsed.data;

  const ann = await prisma.annotation.findUnique({ where: { id: input.annotationId } });
  if (!ann) return { error: "Annotation not found." };

  const content = {
    premise: ann.premise,
    hypothesis: ann.hypothesis,
    label: ann.label,
    mettaPremise: ann.mettaPremise,
    mettaHypothesis: ann.mettaHypothesis,
  };
  const stamp = { reviewerId: rid, reviewerName: rname };

  const cleanup = [
    prisma.correct.deleteMany({ where: { annotationId: input.annotationId, reviewerId: rid } }),
    prisma.failed.deleteMany({ where: { annotationId: input.annotationId, reviewerId: rid } }),
    prisma.final.deleteMany({ where: { sourceAnnotationId: input.annotationId, reviewerId: rid } }),
  ];

  if (isCorrect(input.premiseOk, input.hypothesisOk)) {
    await prisma.$transaction([
      ...cleanup,
      prisma.correct.create({
        data: {
          annotationId: input.annotationId,
          ...stamp,
          ...content,
          note: normalizeNote(input.note),
        },
      }),
      prisma.final.create({
        data: {
          sourceAnnotationId: input.annotationId,
          ...stamp,
          ...content,
          origin: "DIRECT",
          note: normalizeNote(input.note),
        },
      }),
    ]);
  } else {
    await prisma.$transaction([
      ...cleanup,
      prisma.failed.create({
        data: {
          annotationId: input.annotationId,
          ...stamp,
          ...content,
          premiseFailed: !input.premiseOk,
          hypothesisFailed: !input.hypothesisOk,
          note: normalizeNote(input.note),
        },
      }),
    ]);
  }

  return { ok: true };
}
