import { beforeEach, describe, expect, it } from "vitest";
import { submitFix } from "@/app/failed/actions";
import { submitVerdict } from "@/app/review/actions";
import { hasTestDb, prisma, seedAnnotation, truncateAll } from "../helpers/db";

describe.skipIf(!hasTestDb)("data-integrity invariants", () => {
  beforeEach(async () => {
    await truncateAll();
  });

  it("a pair is never in both Correct and Failed at once", async () => {
    const id = await seedAnnotation();
    await submitVerdict(id, true, true, "");
    await submitVerdict(id, false, true, ""); // flip
    await submitVerdict(id, true, true, ""); // flip back

    const inCorrect = await prisma.correct.count({ where: { annotationId: id } });
    const inFailed = await prisma.failed.count({ where: { annotationId: id } });
    expect(inCorrect + inFailed).toBe(1);
  });

  it("count(Final) == count(Correct) + count(Fixed), no orphan Final(FIXED)", async () => {
    const a0 = await seedAnnotation({ sourceIndex: 0 });
    const a1 = await seedAnnotation({ sourceIndex: 1 });
    const a2 = await seedAnnotation({ sourceIndex: 2 });
    await submitVerdict(a0, true, true, "");
    await submitVerdict(a1, false, false, "");
    await submitVerdict(a2, true, false, "");
    const f1 = await prisma.failed.findFirstOrThrow({ where: { annotationId: a1 } });
    await submitFix(f1.id, "(p)", "(h)");

    const [finalC, correctC, fixedC, finalFixedC] = await Promise.all([
      prisma.final.count(),
      prisma.correct.count(),
      prisma.fixed.count(),
      prisma.final.count({ where: { origin: "FIXED" } }),
    ]);
    expect(finalC).toBe(correctC + fixedC);
    expect(finalFixedC).toBe(fixedC); // every Final(FIXED) has a Fixed
  });

  it("every fixed Failed row has isFixed = true", async () => {
    const id = await seedAnnotation();
    await submitVerdict(id, false, true, "");
    const f = await prisma.failed.findFirstOrThrow();
    await submitFix(f.id, "(p)", "(h)");

    const fixedRows = await prisma.fixed.findMany({ include: { failed: true } });
    for (const fx of fixedRows) expect(fx.failed.isFixed).toBe(true);
  });
});

describe.skipIf(!hasTestDb)("database constraints", () => {
  beforeEach(async () => {
    await truncateAll();
  });

  it("one verdict per (annotation, reviewer) — duplicate for same reviewer rejected", async () => {
    const id = await seedAnnotation();
    const base = {
      annotationId: id,
      reviewerId: "rev-a",
      premise: "p",
      hypothesis: "h",
      label: "entailment",
      mettaPremise: "(a)",
      mettaHypothesis: "(b)",
    };
    await prisma.correct.create({ data: base });
    await expect(prisma.correct.create({ data: base })).rejects.toThrow();
  });

  it("two different reviewers can each review the same annotation", async () => {
    const id = await seedAnnotation();
    const base = {
      annotationId: id,
      premise: "p",
      hypothesis: "h",
      label: "entailment",
      mettaPremise: "(a)",
      mettaHypothesis: "(b)",
    };
    await prisma.correct.create({ data: { ...base, reviewerId: "rev-a" } });
    await prisma.correct.create({ data: { ...base, reviewerId: "rev-b" } });
    expect(await prisma.correct.count({ where: { annotationId: id } })).toBe(2);
  });

  it("deleting an Annotation cascades to all verdict tables", async () => {
    const id = await seedAnnotation();
    await submitVerdict(id, false, true, "");
    const f = await prisma.failed.findFirstOrThrow();
    await submitFix(f.id, "(p)", "(h)");

    await prisma.annotation.delete({ where: { id } });

    expect(await prisma.failed.count()).toBe(0);
    expect(await prisma.fixed.count()).toBe(0);
    expect(await prisma.final.count()).toBe(0);
    expect(await prisma.correct.count()).toBe(0);
  });

  it("deleting a Failed row cascades to its Fixed row", async () => {
    const id = await seedAnnotation();
    await submitVerdict(id, false, true, "");
    const f = await prisma.failed.findFirstOrThrow();
    await submitFix(f.id, "(p)", "(h)");

    await prisma.failed.delete({ where: { id: f.id } });
    expect(await prisma.fixed.count()).toBe(0);
  });

  it("Final.origin rejects values outside the enum", async () => {
    const id = await seedAnnotation();
    await expect(
      prisma.final.create({
        data: {
          sourceAnnotationId: id,
          reviewerId: "local",
          premise: "p",
          hypothesis: "h",
          label: "entailment",
          mettaPremise: "(a)",
          mettaHypothesis: "(b)",
          // deliberately invalid enum value
          origin: "BOGUS" as never,
        },
      }),
    ).rejects.toThrow();
  });
});
