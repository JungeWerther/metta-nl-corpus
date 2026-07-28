import { beforeEach, describe, expect, it } from "vitest";
import { getReviewData, submitVerdict } from "@/app/review/actions";
import { hasTestDb, prisma, seedAnnotation, truncateAll } from "../helpers/db";

describe.skipIf(!hasTestDb)("submitVerdict (integration)", () => {
  beforeEach(async () => {
    await truncateAll();
  });

  it("both Yes -> one Correct + one Final(DIRECT), no Failed", async () => {
    const id = await seedAnnotation();
    const res = await submitVerdict(id, true, true, "");
    expect(res).toEqual({ ok: true });

    expect(await prisma.correct.count()).toBe(1);
    expect(await prisma.final.count()).toBe(1);
    expect(await prisma.failed.count()).toBe(0);

    const correct = await prisma.correct.findFirstOrThrow();
    const ann = await prisma.annotation.findUniqueOrThrow({ where: { id } });
    expect(correct.premise).toBe(ann.premise);
    expect(correct.mettaPremise).toBe(ann.mettaPremise);

    const fin = await prisma.final.findFirstOrThrow();
    expect(fin.origin).toBe("DIRECT");
    expect(fin.sourceAnnotationId).toBe(id);
  });

  it("premise No only -> Failed(premiseFailed=true, hypothesisFailed=false)", async () => {
    const id = await seedAnnotation();
    await submitVerdict(id, false, true, "wrong entity");

    expect(await prisma.correct.count()).toBe(0);
    expect(await prisma.final.count()).toBe(0);
    const f = await prisma.failed.findFirstOrThrow();
    expect(f.premiseFailed).toBe(true);
    expect(f.hypothesisFailed).toBe(false);
    expect(f.note).toBe("wrong entity");
  });

  it("hypothesis No only -> Failed(false, true)", async () => {
    const id = await seedAnnotation();
    await submitVerdict(id, true, false, "");
    const f = await prisma.failed.findFirstOrThrow();
    expect(f.premiseFailed).toBe(false);
    expect(f.hypothesisFailed).toBe(true);
    expect(f.note).toBeNull(); // empty note normalized to null
  });

  it("both No -> Failed(true, true)", async () => {
    const id = await seedAnnotation();
    await submitVerdict(id, false, false, "   "); // whitespace note -> null
    const f = await prisma.failed.findFirstOrThrow();
    expect(f.premiseFailed).toBe(true);
    expect(f.hypothesisFailed).toBe(true);
    expect(f.note).toBeNull();
  });

  it("is idempotent (submitting the same verdict twice keeps one row)", async () => {
    const id = await seedAnnotation();
    await submitVerdict(id, true, true, "");
    await submitVerdict(id, true, true, "");
    expect(await prisma.correct.count()).toBe(1);
    expect(await prisma.final.count()).toBe(1);
  });

  it("transition Yes -> No removes Correct/Final and creates Failed", async () => {
    const id = await seedAnnotation();
    await submitVerdict(id, true, true, "");
    await submitVerdict(id, false, true, "changed my mind");
    expect(await prisma.correct.count()).toBe(0);
    expect(await prisma.final.count()).toBe(0);
    expect(await prisma.failed.count()).toBe(1);
  });

  it("transition No -> Yes removes Failed (and cascades) and creates Correct/Final", async () => {
    const id = await seedAnnotation();
    await submitVerdict(id, false, false, "");
    await submitVerdict(id, true, true, "");
    expect(await prisma.failed.count()).toBe(0);
    expect(await prisma.fixed.count()).toBe(0);
    expect(await prisma.correct.count()).toBe(1);
    expect(await prisma.final.count()).toBe(1);
  });

  it("unknown annotationId -> error, no writes", async () => {
    const res = await submitVerdict("does-not-exist", true, true, "");
    expect(res).toHaveProperty("error");
    expect(await prisma.correct.count()).toBe(0);
    expect(await prisma.final.count()).toBe(0);
  });

  it("invalid input (empty id) -> error, no writes", async () => {
    const res = await submitVerdict("", true, true, "");
    expect(res).toHaveProperty("error");
    expect(await prisma.correct.count()).toBe(0);
  });
});

describe.skipIf(!hasTestDb)("getReviewData (integration)", () => {
  beforeEach(async () => {
    await truncateAll();
  });

  it("total=0 -> null annotation", async () => {
    const d = await getReviewData();
    expect(d.total).toBe(0);
    expect(d.annotation).toBeNull();
  });

  it("resumes at the first unreviewed pair", async () => {
    const a0 = await seedAnnotation({ sourceIndex: 0 });
    const a1 = await seedAnnotation({ sourceIndex: 1 });
    await seedAnnotation({ sourceIndex: 2 });

    let d = await getReviewData();
    expect(d.position).toBe(0);
    expect(d.annotation?.id).toBe(a0);

    await submitVerdict(a0, true, true, "");
    d = await getReviewData();
    expect(d.position).toBe(1);
    expect(d.annotation?.id).toBe(a1);
    expect(d.progress.reviewed).toBe(1);
    expect(d.progress.correct).toBe(1);
  });

  it("prefills an existing verdict when navigating back", async () => {
    const a0 = await seedAnnotation({ sourceIndex: 0 });
    await submitVerdict(a0, false, true, "note here");
    const d = await getReviewData(0);
    expect(d.verdict).toEqual({ premiseOk: false, hypothesisOk: true, note: "note here" });
  });

  it("clamps an out-of-range position to the last pair", async () => {
    await seedAnnotation({ sourceIndex: 0 });
    await seedAnnotation({ sourceIndex: 1 });
    const d = await getReviewData(999);
    expect(d.position).toBe(1);
    expect(d.annotation).not.toBeNull();
  });
});
