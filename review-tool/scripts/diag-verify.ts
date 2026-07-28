import { readFileSync } from "node:fs";
import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaClient } from "../lib/generated/prisma/client";

function dbUrl(): string {
  const line = readFileSync(".env.local", "utf8").split("\n").find((l) => l.startsWith("DATABASE_URL="));
  if (!line) throw new Error("no DATABASE_URL in .env.local");
  return line.slice("DATABASE_URL=".length).trim().replace(/^["']|["']$/g, "");
}
const prisma = new PrismaClient({ adapter: new PrismaPg({ connectionString: dbUrl() }) });
const ORDER = [{ sourceIndex: "asc" as const }, { id: "asc" as const }];
const TARGET = "3243faae-5967-46e5-8613-2831618d8208";

async function main() {
  const ordered = await prisma.annotation.findMany({ orderBy: ORDER, select: { id: true } });
  const realPos = ordered.findIndex((a) => a.id === TARGET);

  const [c, f] = await Promise.all([
    prisma.correct.findFirst({ where: { annotationId: TARGET }, select: { reviewerId: true } }),
    prisma.failed.findFirst({ where: { annotationId: TARGET }, select: { reviewerId: true } }),
  ]);

  console.log(`TARGET ${TARGET}`);
  console.log(`  real ORDER position: pair ${realPos + 1} (index ${realPos}) of ${ordered.length}`);
  console.log(`  reviewed? correct=${!!c} failed=${!!f}`);

  // how many distinct reviewers, and each one's reviewed count
  const [cr, fr] = await Promise.all([
    prisma.correct.groupBy({ by: ["reviewerId"], _count: true }),
    prisma.failed.groupBy({ by: ["reviewerId"], _count: true }),
  ]);
  console.log("\nreviewers:");
  const ids = new Set([...cr, ...fr].map((r) => r.reviewerId));
  for (const rid of ids) {
    const cc = cr.find((x) => x.reviewerId === rid)?._count ?? 0;
    const ff = fr.find((x) => x.reviewerId === rid)?._count ?? 0;
    console.log(`  ${rid}: ${(cc as number) + (ff as number)} reviewed (${cc} ✓ / ${ff} ✗)`);
  }
}
main().finally(() => prisma.$disconnect());
