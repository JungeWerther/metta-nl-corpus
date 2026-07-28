import { readFileSync, writeFileSync } from "node:fs";
import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaClient } from "../lib/generated/prisma/client";

function dbUrl(): string {
  const line = readFileSync(".env.local", "utf8").split("\n").find((l) => l.startsWith("DATABASE_URL="));
  if (!line) throw new Error("no DATABASE_URL in .env.local");
  return line.slice("DATABASE_URL=".length).trim().replace(/^["']|["']$/g, "");
}
const prisma = new PrismaClient({ adapter: new PrismaPg({ connectionString: dbUrl() }) });
const ORDER = [{ sourceIndex: "asc" as const }, { id: "asc" as const }];

const N = Number(process.argv[2] ?? 10);

async function main() {
  // Reviewer identity (the one holding all the verdicts). Pick the reviewer with
  // the most reviews so we write under the same identity the dashboard shows.
  const grp = await prisma.correct.groupBy({ by: ["reviewerId", "reviewerName"], _count: true });
  const grpF = await prisma.failed.groupBy({ by: ["reviewerId", "reviewerName"], _count: true });
  const tally = new Map<string, { name: string | null; n: number }>();
  for (const g of [...grp, ...grpF]) {
    const cur = tally.get(g.reviewerId) ?? { name: g.reviewerName, n: 0 };
    cur.n += g._count as number;
    if (g.reviewerName) cur.name = g.reviewerName;
    tally.set(g.reviewerId, cur);
  }
  const [rid, info] = [...tally.entries()].sort((a, b) => b[1].n - a[1].n)[0];

  const [correct, failed, total] = await Promise.all([
    prisma.correct.count({ where: { reviewerId: rid } }),
    prisma.failed.count({ where: { reviewerId: rid } }),
    prisma.annotation.count(),
  ]);

  const anns = await prisma.annotation.findMany({
    where: { corrects: { none: { reviewerId: rid } }, faileds: { none: { reviewerId: rid } } },
    orderBy: ORDER,
    take: N,
    select: { id: true, premise: true, hypothesis: true, label: true, mettaPremise: true, mettaHypothesis: true, sourceIndex: true },
  });

  console.log(`REVIEWER: ${rid}  (name: ${info.name ?? "—"})`);
  console.log(`COUNTS:   ${correct + failed} reviewed / ${total}   (${correct} ✓ / ${failed} ✗)`);

  const out = process.argv.find((a) => a.endsWith(".json"));
  if (out) {
    writeFileSync(out, JSON.stringify(anns, null, 2));
    console.log(`wrote ${anns.length} pairs -> ${out}`);
    return;
  }

  console.log(`\nNEXT ${anns.length} UNREVIEWED PAIRS:\n${"=".repeat(70)}`);
  anns.forEach((a, i) => {
    console.log(`\n[${i + 1}] id=${a.id}  sourceIndex=${a.sourceIndex}  label=${a.label}`);
    console.log(`  PREMISE (EN):    ${a.premise}`);
    console.log(`  PREMISE (MeTTa): ${a.mettaPremise}`);
    console.log(`  HYPOTH  (EN):    ${a.hypothesis}`);
    console.log(`  HYPOTH  (MeTTa): ${a.mettaHypothesis}`);
  });
}
main().finally(() => prisma.$disconnect());
