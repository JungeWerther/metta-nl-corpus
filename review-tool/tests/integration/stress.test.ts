import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { submitVerdict } from "@/app/review/actions";
import { getStats } from "@/app/dashboard/actions";
import { hasTestDb, prisma, seedAnnotation, truncateAll } from "../helpers/db";

const ORIGINAL = process.env.REVIEW_TEST_USER;
const asReviewer = (id: string) => {
  process.env.REVIEW_TEST_USER = id;
};

describe.skipIf(!hasTestDb)("stress / load", () => {
  beforeEach(async () => {
    await truncateAll();
  });
  afterEach(() => {
    process.env.REVIEW_TEST_USER = ORIGINAL;
  });

  it(
    "handles a burst of 40 parallel submits without loss or corruption",
    async () => {
      asReviewer("stress");
      const ids = await Promise.all(
        Array.from({ length: 40 }, (_, i) => seedAnnotation({ sourceIndex: i })),
      );

      const start = Date.now();
      const results = await Promise.allSettled(
        // ~2/3 correct, ~1/3 failed
        ids.map((id, i) => submitVerdict(id, i % 3 !== 0, true, "")),
      );
      const ms = Date.now() - start;

      const ok = results.filter(
        (r) => r.status === "fulfilled" && !("error" in (r.value as object)),
      ).length;
      const sampleErr = results.find((r) => r.status === "rejected");
      // eslint-disable-next-line no-console
      console.log(
        `[stress] 40 parallel submits in ${ms}ms — ok=${ok} — sampleErr=`,
        sampleErr && sampleErr.status === "rejected"
          ? String((sampleErr.reason as Error)?.message ?? sampleErr.reason)
              .replace(/\s+/g, " ")
              .slice(0, 320)
          : "none",
      );

      expect(ok).toBe(40); // no submit lost / errored under load

      const [correct, failed] = await Promise.all([
        prisma.correct.count({ where: { reviewerId: "stress" } }),
        prisma.failed.count({ where: { reviewerId: "stress" } }),
      ]);
      expect(correct + failed).toBe(40); // exactly one verdict per pair, no dupes

      const stats = await getStats();
      expect(stats.reviewed).toBe(40);
      expect(stats.gold).toBe(correct); // Final(DIRECT) count matches Correct
    },
    60_000,
  );
});
