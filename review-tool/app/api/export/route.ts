import { NextRequest, NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";
import { getReviewer } from "@/lib/reviewer";
import { toCsv } from "@/lib/export";

const TABLES = ["final", "correct", "failed", "fixed"] as const;
type Table = (typeof TABLES)[number];

function isTable(t: string): t is Table {
  return (TABLES as readonly string[]).includes(t);
}

// Rows are scoped to the current reviewer.
async function fetchRows(table: Table, rid: string) {
  switch (table) {
    case "final":
      return prisma.final.findMany({ where: { reviewerId: rid } });
    case "correct":
      return prisma.correct.findMany({ where: { reviewerId: rid } });
    case "failed":
      return prisma.failed.findMany({ where: { reviewerId: rid } });
    case "fixed":
      return prisma.fixed.findMany({ where: { reviewerId: rid } });
  }
}

export async function GET(req: NextRequest) {
  const { id: rid } = await getReviewer();

  const table = req.nextUrl.searchParams.get("table") ?? "final";
  const format = req.nextUrl.searchParams.get("format") ?? "json";
  if (!isTable(table)) {
    return NextResponse.json({ error: `Unknown table '${table}'` }, { status: 400 });
  }

  const rows = await fetchRows(table, rid);
  const filename = `${table}_${new Date().toISOString().slice(0, 10)}`;

  if (format === "csv") {
    return new NextResponse(toCsv(rows as Record<string, unknown>[]), {
      headers: {
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition": `attachment; filename="${filename}.csv"`,
      },
    });
  }

  return new NextResponse(JSON.stringify(rows, null, 2), {
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Content-Disposition": `attachment; filename="${filename}.json"`,
    },
  });
}
