import { beforeEach, describe, expect, it } from "vitest";
import { getFailedList, revertFix, submitFix } from "@/app/failed/actions";
import { submitVerdict } from "@/app/review/actions";
import { hasTestDb, prisma, seedAnnotation, truncateAll } from "../helpers/db";

async function makeFailed(): Promise<{ annotationId: string; failedId: string }> {
  const annotationId = await seedAnnotation();
  await submitVerdict(annotationId, true, false, ""); // hypothesis failed
  const f = await prisma.failed.findFirstOrThrow({ where: { annotationId } });
  return { annotationId, failedId: f.id };
}

describe.skipIf(!hasTestDb)("submitFix / revertFix (integration)", () => {
  beforeEach(async () => {
    await truncateAll();
  });

  it("writes Fixed(original+fixed) + Final(FIXED) and flags isFixed", async () => {
    const { annotationId, failedId } = await makeFailed();
    const original = await prisma.failed.findUniqueOrThrow({ where: { id: failedId } });

    const res = await submitFix(failedId, "(fixed premise)", "(fixed hypothesis)");
    expect(res).toEqual({ ok: true });

    const fixed = await prisma.fixed.findFirstOrThrow();
    expect(fixed.oldUuid).toBe(annotationId);
    expect(fixed.originalMettaPremise).toBe(original.mettaPremise);
    expect(fixed.fixedMettaPremise).toBe("(fixed premise)");
    expect(fixed.fixedMettaHypothesis).toBe("(fixed hypothesis)");

    const fin = await prisma.final.findFirstOrThrow({ where: { origin: "FIXED" } });
    expect(fin.mettaPremise).toBe("(fixed premise)"); // fixed, not original
    expect(fin.sourceAnnotationId).toBe(annotationId);

    const failed = await prisma.failed.findUniqueOrThrow({ where: { id: failedId } });
    expect(failed.isFixed).toBe(true);
  });

  it("re-fixing replaces the prior fix (no duplicates)", async () => {
    const { failedId } = await makeFailed();
    await submitFix(failedId, "(v1 p)", "(v1 h)");
    await submitFix(failedId, "(v2 p)", "(v2 h)");

    expect(await prisma.fixed.count()).toBe(1);
    expect(await prisma.final.count({ where: { origin: "FIXED" } })).toBe(1);
    const fixed = await prisma.fixed.findFirstOrThrow();
    expect(fixed.fixedMettaPremise).toBe("(v2 p)");
  });

  it("empty fixed MeTTa -> error, no writes", async () => {
    const { failedId } = await makeFailed();
    const res = await submitFix(failedId, "   ", "(h)");
    expect(res).toHaveProperty("error");
    expect(await prisma.fixed.count()).toBe(0);
    expect(await prisma.final.count()).toBe(0);
  });

  it("unknown failedId -> error", async () => {
    const res = await submitFix("nope", "(p)", "(h)");
    expect(res).toHaveProperty("error");
  });

  it("revert removes Fixed + Final(FIXED) and clears isFixed", async () => {
    const { failedId } = await makeFailed();
    await submitFix(failedId, "(p)", "(h)");
    const res = await revertFix(failedId);
    expect(res).toEqual({ ok: true });

    expect(await prisma.fixed.count()).toBe(0);
    expect(await prisma.final.count({ where: { origin: "FIXED" } })).toBe(0);
    const failed = await prisma.failed.findUniqueOrThrow({ where: { id: failedId } });
    expect(failed.isFixed).toBe(false);
  });

  it("getFailedList returns the row with its fix data", async () => {
    const { failedId } = await makeFailed();
    let list = await getFailedList();
    expect(list).toHaveLength(1);
    expect(list[0].hypothesisFailed).toBe(true);
    expect(list[0].isFixed).toBe(false);
    expect(list[0].fixed).toBeNull();

    await submitFix(failedId, "(p)", "(h)");
    list = await getFailedList();
    expect(list[0].isFixed).toBe(true);
    expect(list[0].fixed?.fixedMettaPremise).toBe("(p)");
  });

  it("re-verdicting a fixed pair to Correct cleans up Fixed + Final(FIXED)", async () => {
    const { annotationId, failedId } = await makeFailed();
    await submitFix(failedId, "(p)", "(h)");
    // now change the original verdict to both-Yes
    await submitVerdict(annotationId, true, true, "");

    expect(await prisma.failed.count()).toBe(0);
    expect(await prisma.fixed.count()).toBe(0);
    expect(await prisma.final.count({ where: { origin: "FIXED" } })).toBe(0);
    expect(await prisma.correct.count()).toBe(1);
    expect(await prisma.final.count({ where: { origin: "DIRECT" } })).toBe(1);
  });
});
