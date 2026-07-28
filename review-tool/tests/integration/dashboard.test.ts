import { beforeEach, describe, expect, it } from "vitest";
import { getStats } from "@/app/dashboard/actions";
import { submitFix } from "@/app/failed/actions";
import { submitVerdict } from "@/app/review/actions";
import { hasTestDb, prisma, seedAnnotation, truncateAll } from "../helpers/db";

describe.skipIf(!hasTestDb)("getStats (integration)", () => {
  beforeEach(async () => {
    await truncateAll();
  });

  it("counts a mixed review set correctly", async () => {
    const a0 = await seedAnnotation({ sourceIndex: 0 }); // correct
    const a1 = await seedAnnotation({ sourceIndex: 1 }); // premise fail -> fix
    const a2 = await seedAnnotation({ sourceIndex: 2 }); // hypothesis fail
    const a3 = await seedAnnotation({ sourceIndex: 3 }); // both fail
    await seedAnnotation({ sourceIndex: 4 }); // unreviewed

    await submitVerdict(a0, true, true, "");
    await submitVerdict(a1, false, true, "");
    await submitVerdict(a2, true, false, "");
    await submitVerdict(a3, false, false, "");

    const f1 = await prisma.failed.findFirstOrThrow({ where: { annotationId: a1 } });
    await submitFix(f1.id, "(p)", "(h)");

    const s = await getStats();
    expect(s.total).toBe(5);
    expect(s.reviewed).toBe(4);
    expect(s.correct).toBe(1);
    expect(s.failed).toBe(3);
    expect(s.fixed).toBe(1);
    expect(s.toFix).toBe(2);
    expect(s.gold).toBe(2); // a0 (DIRECT) + a1 (FIXED)
    expect(s.goldDirect).toBe(1);
    expect(s.goldFixed).toBe(1);
    expect(s.premiseNo).toBe(2); // a1, a3
    expect(s.premiseYes).toBe(2);
    expect(s.hypothesisNo).toBe(2); // a2, a3
    expect(s.hypothesisYes).toBe(2);
  });

  it("is safe with zero reviews", async () => {
    await seedAnnotation();
    const s = await getStats();
    expect(s.total).toBe(1);
    expect(s.reviewed).toBe(0);
    expect(s.premiseYes).toBe(0);
    expect(s.gold).toBe(0);
  });
});
