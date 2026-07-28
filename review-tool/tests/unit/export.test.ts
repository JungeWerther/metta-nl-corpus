import { describe, expect, it } from "vitest";
import { toCsv } from "@/lib/export";

describe("toCsv", () => {
  it("returns empty string for no rows", () => {
    expect(toCsv([])).toBe("");
  });

  it("writes a header row + data rows", () => {
    const csv = toCsv([
      { a: "x", b: 1 },
      { a: "y", b: 2 },
    ]);
    expect(csv).toBe('a,b\n"x","1"\n"y","2"');
  });

  it("handles null/undefined as empty, booleans, and Dates as ISO", () => {
    const csv = toCsv([
      { a: null, b: true, c: new Date("2026-01-02T03:04:05.000Z") },
    ]);
    expect(csv).toBe('a,b,c\n,"true","2026-01-02T03:04:05.000Z"');
  });

  it("escapes quotes and preserves commas/newlines inside quoted cells", () => {
    const csv = toCsv([{ a: 'he said "hi"', b: "x,y\nz" }]);
    expect(csv).toBe('a,b\n"he said ""hi""","x,y\nz"');
  });

  it("serializes nested objects as JSON", () => {
    const csv = toCsv([{ a: { k: 1 } }]);
    expect(csv).toBe('a\n"{""k"":1}"');
  });
});
