import { readFileSync } from "node:fs";
import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaClient } from "../lib/generated/prisma/client";

function dbUrl(): string {
  const line = readFileSync(".env.local", "utf8").split("\n").find((l) => l.startsWith("DATABASE_URL="));
  if (!line) throw new Error("no DATABASE_URL in .env.local");
  return line.slice("DATABASE_URL=".length).trim().replace(/^["']|["']$/g, "");
}
const prisma = new PrismaClient({ adapter: new PrismaPg({ connectionString: dbUrl() }) });

// The annotationIds this session actually wrote (batches 1, 2, 3).
const SCRATCH = "/private/tmp/claude-501/-Users-manasraaj/3dc44db7-f445-4cf5-925f-10cd630320c2/scratchpad/";

async function main() {
  const rid = "f0861c51-248a-4a23-b69d-325bdbe14290";

  // All premise-incorrect verdicts that currently exist for this reviewer.
  const premiseFails = await prisma.failed.findMany({
    where: { reviewerId: rid, premiseFailed: true },
    select: { annotationId: true },
  });
  const premiseFailIds = new Set(premiseFails.map((r) => r.annotationId));

  // The 100 ids from this session's big batch.
  const batch100 = JSON.parse(readFileSync(SCRATCH + "pending.json", "utf8")).map((p: any) => p.id);
  const batch100Set = new Set<string>(batch100);

  // Overlap: did any premise-incorrect verdict land inside this session's batch?
  const overlap = [...premiseFailIds].filter((id) => batch100Set.has(id));

  const [totalFailed, premiseFailCount, hypFailCount] = await Promise.all([
    prisma.failed.count({ where: { reviewerId: rid } }),
    prisma.failed.count({ where: { reviewerId: rid, premiseFailed: true } }),
    prisma.failed.count({ where: { reviewerId: rid, hypothesisFailed: true } }),
  ]);

  console.log(`reviewer ${rid}`);
  console.log(`Failed rows total:            ${totalFailed}`);
  console.log(`  premise marked incorrect:  ${premiseFailCount}`);
  console.log(`  hypothesis marked incorrect: ${hypFailCount}`);
  console.log("");
  console.log(`This session's batch size:   ${batch100.length} (+ 20 from batches 1-2)`);
  console.log(`premise-incorrect verdicts that fall inside this session's 100-batch: ${overlap.length}`);
  console.log(overlap.length === 0
    ? "=> NONE of your premise-incorrect verdicts are among the pairs I wrote."
    : `=> OVERLAP: ${overlap.join(", ")}`);
}
main().finally(() => prisma.$disconnect());
