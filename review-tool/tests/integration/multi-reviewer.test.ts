import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { getReviewData, submitVerdict } from "@/app/review/actions";
import { submitFix } from "@/app/failed/actions";
import { hasTestDb, prisma, seedAnnotation, truncateAll } from "../helpers/db";

// The actions resolve the reviewer from REVIEW_TEST_USER in tests, so we can
// simulate different reviewers by swapping it before each call.
const ORIGINAL = process.env.REVIEW_TEST_USER;
function asReviewer(id: string) {
  process.env.REVIEW_TEST_USER = id;
}

describe.skipIf(!hasTestDb)("multi-reviewer + attribution", () => {
  beforeEach(async () => {
    await truncateAll();
  });
  afterEach(() => {
    process.env.REVIEW_TEST_USER = ORIGINAL;
  });

  it("stamps reviewerId + reviewerName on Correct and Final", async () => {
    const id = await seedAnnotation();
    asReviewer("alice");
    await submitVerdict(id, true, true, "");

    const c = await prisma.correct.findFirstOrThrow();
    expect(c.reviewerId).toBe("alice");
    expect(c.reviewerName).toBe("alice");
    const f = await prisma.final.findFirstOrThrow();
    expect(f.reviewerId).toBe("alice");
    expect(f.reviewerName).toBe("alice");
  });

  it("stamps reviewerName on Failed", async () => {
    const id = await seedAnnotation();
    asReviewer("bob");
    await submitVerdict(id, true, false, "");
    const f = await prisma.failed.findFirstOrThrow();
    expect(f.reviewerId).toBe("bob");
    expect(f.reviewerName).toBe("bob");
  });

  it("stamps reviewerName on Fixed", async () => {
    const id = await seedAnnotation();
    asReviewer("carol");
    await submitVerdict(id, false, true, "");
    const failed = await prisma.failed.findFirstOrThrow();
    await submitFix(failed.id, "(p)", "(h)");
    const fixed = await prisma.fixed.findFirstOrThrow();
    expect(fixed.reviewerId).toBe("carol");
    expect(fixed.reviewerName).toBe("carol");
  });

  it("two reviewers review the SAME annotation independently (no conflict)", async () => {
    const id = await seedAnnotation();
    asReviewer("alice");
    await submitVerdict(id, true, true, ""); // alice: correct
    asReviewer("bob");
    await submitVerdict(id, true, false, "b"); // bob: failed

    expect(await prisma.correct.count({ where: { annotationId: id } })).toBe(1);
    expect(await prisma.failed.count({ where: { annotationId: id } })).toBe(1);
    const c = await prisma.correct.findFirstOrThrow({ where: { annotationId: id } });
    expect(c.reviewerId).toBe("alice");
    const f = await prisma.failed.findFirstOrThrow({ where: { annotationId: id } });
    expect(f.reviewerId).toBe("bob");
  });

  it("resume position is per-reviewer", async () => {
    const a0 = await seedAnnotation({ sourceIndex: 0 });
    await seedAnnotation({ sourceIndex: 1 });

    asReviewer("alice");
    await submitVerdict(a0, true, true, "");
    const aliceData = await getReviewData();
    expect(aliceData.position).toBe(1); // alice resumes at the 2nd pair
    expect(aliceData.progress.reviewed).toBe(1);

    asReviewer("bob"); // bob has reviewed nothing
    const bobData = await getReviewData();
    expect(bobData.position).toBe(0);
    expect(bobData.progress.reviewed).toBe(0);
  });

  it("one reviewer changing a verdict does not affect the other", async () => {
    const id = await seedAnnotation();
    asReviewer("alice");
    await submitVerdict(id, true, true, ""); // alice correct
    asReviewer("bob");
    await submitVerdict(id, true, true, ""); // bob correct
    asReviewer("alice");
    await submitVerdict(id, false, true, ""); // alice flips to failed

    expect(await prisma.correct.count({ where: { annotationId: id, reviewerId: "bob" } })).toBe(1);
    expect(await prisma.correct.count({ where: { annotationId: id, reviewerId: "alice" } })).toBe(0);
    expect(await prisma.failed.count({ where: { annotationId: id, reviewerId: "alice" } })).toBe(1);
  });
});
