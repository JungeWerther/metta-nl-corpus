"use server";

import { prisma } from "@/lib/prisma";
import { getReviewer } from "@/lib/reviewer";
import { computeStats } from "@/lib/review-logic";
import type { Stats } from "./types";

export async function getStats(): Promise<Stats> {
  const { id: rid } = await getReviewer();
  const [total, correct, failed, premiseFailed, hypothesisFailed, fixed, gold, goldFixed] =
    await Promise.all([
      prisma.annotation.count(),
      prisma.correct.count({ where: { reviewerId: rid } }),
      prisma.failed.count({ where: { reviewerId: rid } }),
      prisma.failed.count({ where: { reviewerId: rid, premiseFailed: true } }),
      prisma.failed.count({ where: { reviewerId: rid, hypothesisFailed: true } }),
      prisma.failed.count({ where: { reviewerId: rid, isFixed: true } }),
      prisma.final.count({ where: { reviewerId: rid } }),
      prisma.final.count({ where: { reviewerId: rid, origin: "FIXED" } }),
    ]);

  return computeStats({
    total,
    correct,
    failed,
    premiseFailed,
    hypothesisFailed,
    fixed,
    gold,
    goldFixed,
  });
}
