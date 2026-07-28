// Convert an array of flat record objects to CSV. Pure + unit-tested.
// Handles nulls (empty), Dates (ISO), objects (JSON), and quote escaping.
export function toCsv(rows: Record<string, unknown>[]): string {
  if (rows.length === 0) return "";
  const headers = Array.from(new Set(rows.flatMap((r) => Object.keys(r))));

  const cell = (v: unknown): string => {
    if (v === null || v === undefined) return "";
    const s =
      v instanceof Date
        ? v.toISOString()
        : typeof v === "object"
          ? JSON.stringify(v)
          : String(v);
    return `"${s.replace(/"/g, '""')}"`;
  };

  const lines = [
    headers.join(","),
    ...rows.map((r) => headers.map((h) => cell(r[h])).join(",")),
  ];
  return lines.join("\n");
}
