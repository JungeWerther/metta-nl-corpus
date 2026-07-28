import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { NextRequest } from "next/server";
import { GET } from "@/app/api/export/route";
import { submitVerdict } from "@/app/review/actions";
import { hasTestDb, seedAnnotation, truncateAll } from "../helpers/db";

const ORIGINAL = process.env.REVIEW_TEST_USER;
const asReviewer = (id: string) => {
  process.env.REVIEW_TEST_USER = id;
};
const call = (qs: string) => GET(new NextRequest(`http://localhost/api/export?${qs}`));

describe.skipIf(!hasTestDb)("export route", () => {
  beforeEach(async () => {
    await truncateAll();
  });
  afterEach(() => {
    process.env.REVIEW_TEST_USER = ORIGINAL;
  });

  it("downloads final as JSON with an attachment header", async () => {
    const id = await seedAnnotation();
    asReviewer("alice");
    await submitVerdict(id, true, true, "");

    const res = await call("table=final&format=json");
    expect(res.status).toBe(200);
    expect(res.headers.get("content-disposition")).toContain('filename="final_');
    expect(res.headers.get("content-type")).toContain("application/json");
    const rows = JSON.parse(await res.text());
    expect(rows).toHaveLength(1);
    expect(rows[0].reviewerId).toBe("alice");
    expect(rows[0].origin).toBe("DIRECT");
  });

  it("downloads a table as CSV with a header row", async () => {
    const id = await seedAnnotation();
    asReviewer("alice");
    await submitVerdict(id, true, false, "");
    const res = await call("table=failed&format=csv");
    expect(res.headers.get("content-type")).toContain("text/csv");
    const text = await res.text();
    expect(text.split("\n")[0]).toContain("premiseFailed");
  });

  it("rejects an unknown table with 400", async () => {
    asReviewer("alice");
    const res = await call("table=secrets&format=json");
    expect(res.status).toBe(400);
  });

  it("only exports the requesting reviewer's rows (no IDOR)", async () => {
    const id = await seedAnnotation();
    asReviewer("alice");
    await submitVerdict(id, true, true, "");

    asReviewer("bob");
    const res = await call("table=final&format=json");
    const rows = JSON.parse(await res.text());
    expect(rows).toHaveLength(0);
  });
});
