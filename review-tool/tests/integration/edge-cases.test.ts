import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { getReviewData, submitVerdict } from "@/app/review/actions";
import { getFailedList, revertFix, submitFix } from "@/app/failed/actions";
import { getStats } from "@/app/dashboard/actions";
import { hasTestDb, prisma, seedAnnotation, truncateAll } from "../helpers/db";

const ORIGINAL = process.env.REVIEW_TEST_USER;
const asReviewer = (id: string) => {
  process.env.REVIEW_TEST_USER = id;
};

describe.skipIf(!hasTestDb)("edge cases, concurrency, scoping", () => {
  beforeEach(async () => {
    await truncateAll();
  });
  afterEach(() => {
    process.env.REVIEW_TEST_USER = ORIGINAL;
  });

  it("stores unicode + embedded quotes in MeTTa without corruption", async () => {
    const id = await seedAnnotation({
      mettaPremise: '(λ 你好 α) (says "hi")',
      mettaHypothesis: "(β)",
    });
    asReviewer("u");
    await submitVerdict(id, true, true, "");
    const c = await prisma.correct.findFirstOrThrow();
    expect(c.mettaPremise).toBe('(λ 你好 α) (says "hi")');
  });

  it("rejects an oversized note (validation) and writes nothing", async () => {
    const id = await seedAnnotation();
    asReviewer("u");
    const res = await submitVerdict(id, true, false, "x".repeat(5000));
    expect(res).toHaveProperty("error");
    expect(await prisma.failed.count()).toBe(0);
  });

  it("handles many parallel submits (same reviewer, different pairs)", async () => {
    asReviewer("racer");
    const ids = await Promise.all(
      Array.from({ length: 15 }, (_, i) => seedAnnotation({ sourceIndex: i })),
    );
    await Promise.all(ids.map((id) => submitVerdict(id, true, true, "")));
    expect(await prisma.correct.count({ where: { reviewerId: "racer" } })).toBe(15);
    expect(await prisma.final.count({ where: { reviewerId: "racer" } })).toBe(15);
  });

  it("a reviewer only sees their own data (scoping / no IDOR)", async () => {
    const id = await seedAnnotation();
    asReviewer("alice");
    await submitVerdict(id, true, true, "");

    asReviewer("bob");
    const data = await getReviewData();
    expect(data.progress.reviewed).toBe(0);
    const stats = await getStats();
    expect(stats.correct).toBe(0);
    expect(await getFailedList()).toHaveLength(0);
  });

  it("clamps navigation at both boundaries", async () => {
    await seedAnnotation({ sourceIndex: 0 });
    await seedAnnotation({ sourceIndex: 1 });
    asReviewer("u");
    expect((await getReviewData(999)).position).toBe(1);
    expect((await getReviewData(-5)).position).toBe(0);
  });

  it("empty dataset returns a null annotation, not an error", async () => {
    asReviewer("u");
    const data = await getReviewData();
    expect(data.total).toBe(0);
    expect(data.annotation).toBeNull();
  });

  it("stores a note on a correct verdict (Correct + Final)", async () => {
    const id = await seedAnnotation();
    asReviewer("u");
    await submitVerdict(id, true, true, "looks good");
    const c = await prisma.correct.findFirstOrThrow();
    expect(c.note).toBe("looks good");
    const f = await prisma.final.findFirstOrThrow();
    expect(f.note).toBe("looks good");
  });

  it("prefills the stored note when navigating back to a correct pair", async () => {
    const id = await seedAnnotation({ sourceIndex: 0 });
    asReviewer("u");
    await submitVerdict(id, true, true, "my note");
    const data = await getReviewData(0);
    expect(data.verdict).toEqual({ premiseOk: true, hypothesisOk: true, note: "my note" });
  });

  it("a reviewer cannot fix or revert another reviewer's failed row (IDOR)", async () => {
    const id = await seedAnnotation();
    asReviewer("alice");
    await submitVerdict(id, true, false, ""); // alice creates a Failed row
    const aliceFailed = await prisma.failed.findFirstOrThrow({ where: { reviewerId: "alice" } });

    asReviewer("bob");
    expect(await submitFix(aliceFailed.id, "(x)", "(y)")).toHaveProperty("error");
    expect(await revertFix(aliceFailed.id)).toHaveProperty("error");

    // alice's row is untouched, nothing was written
    const still = await prisma.failed.findUniqueOrThrow({ where: { id: aliceFailed.id } });
    expect(still.isFixed).toBe(false);
    expect(await prisma.fixed.count()).toBe(0);
  });

  it("stores a note on a fix (Fixed + Final)", async () => {
    const id = await seedAnnotation();
    asReviewer("u");
    await submitVerdict(id, true, false, "");
    const failed = await prisma.failed.findFirstOrThrow();
    await submitFix(failed.id, "(p)", "(h)", "fixed the negation");

    const fixed = await prisma.fixed.findFirstOrThrow();
    expect(fixed.note).toBe("fixed the negation");
    const fin = await prisma.final.findFirstOrThrow({ where: { origin: "FIXED" } });
    expect(fin.note).toBe("fixed the negation");
  });
});
