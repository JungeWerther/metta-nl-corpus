import { readFileSync } from "node:fs";
import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaClient } from "../lib/generated/prisma/client";

// Writes reviewer verdicts to the DB, mirroring app/review/actions.ts submitVerdict
// EXACTLY: per pair, delete this reviewer's Correct/Failed/Final rows, then insert
// Correct+Final (both sides OK) or Failed. Safe to re-run (cleanup-then-insert).
//
//   npx tsx scripts/batch-commit.ts <decisions.json> [--dry-run]

function dbUrl(): string {
  const line = readFileSync(".env.local", "utf8").split("\n").find((l) => l.startsWith("DATABASE_URL="));
  if (!line) throw new Error("no DATABASE_URL in .env.local");
  return line.slice("DATABASE_URL=".length).trim().replace(/^["']|["']$/g, "");
}
const prisma = new PrismaClient({ adapter: new PrismaPg({ connectionString: dbUrl() }) });

type Decision = { annotationId: string; premiseOk: boolean; hypothesisOk: boolean; note?: string };
const normalizeNote = (n?: string) => (n && n.trim() ? n.trim() : null);

async function topReviewer() {
  const [c, f] = await Promise.all([
    prisma.correct.groupBy({ by: ["reviewerId", "reviewerName"], _count: true }),
    prisma.failed.groupBy({ by: ["reviewerId", "reviewerName"], _count: true }),
  ]);
  const tally = new Map<string, { name: string | null; n: number }>();
  for (const g of [...c, ...f]) {
    const cur = tally.get(g.reviewerId) ?? { name: g.reviewerName, n: 0 };
    cur.n += g._count as number;
    if (g.reviewerName) cur.name = g.reviewerName;
    tally.set(g.reviewerId, cur);
  }
  const [rid, info] = [...tally.entries()].sort((a, b) => b[1].n - a[1].n)[0];
  return { rid, rname: info.name };
}

async function main() {
  const file = process.argv[2];
  const dryRun = process.argv.includes("--dry-run");
  if (!file) throw new Error("usage: batch-commit.ts <decisions.json> [--dry-run]");
  const decisions: Decision[] = JSON.parse(readFileSync(file, "utf8"));

  const { rid, rname } = await topReviewer();
  const before = (await prisma.correct.count({ where: { reviewerId: rid } })) +
    (await prisma.failed.count({ where: { reviewerId: rid } }));
  console.log(`reviewer ${rid} (${rname})`);
  console.log(`before: ${before} reviewed\n`);

  for (const d of decisions) {
    const ann = await prisma.annotation.findUnique({ where: { id: d.annotationId } });
    if (!ann) { console.log(`  SKIP ${d.annotationId} — not found`); continue; }
    const content = {
      premise: ann.premise, hypothesis: ann.hypothesis, label: ann.label,
      mettaPremise: ann.mettaPremise, mettaHypothesis: ann.mettaHypothesis,
    };
    const stamp = { reviewerId: rid, reviewerName: rname };
    const both = d.premiseOk && d.hypothesisOk;
    const verdict = both ? "CORRECT ✓" : `FAILED ✗ (p=${d.premiseOk ? "ok" : "NO"} h=${d.hypothesisOk ? "ok" : "NO"})`;
    console.log(`  ${dryRun ? "[dry] " : ""}${d.annotationId} -> ${verdict}`);
    if (dryRun) continue;

    const cleanup = [
      prisma.correct.deleteMany({ where: { annotationId: d.annotationId, reviewerId: rid } }),
      prisma.failed.deleteMany({ where: { annotationId: d.annotationId, reviewerId: rid } }),
      prisma.final.deleteMany({ where: { sourceAnnotationId: d.annotationId, reviewerId: rid } }),
    ];
    if (both) {
      await prisma.$transaction([
        ...cleanup,
        prisma.correct.create({ data: { annotationId: d.annotationId, ...stamp, ...content, note: normalizeNote(d.note) } }),
        prisma.final.create({ data: { sourceAnnotationId: d.annotationId, ...stamp, ...content, origin: "DIRECT", note: normalizeNote(d.note) } }),
      ]);
    } else {
      await prisma.$transaction([
        ...cleanup,
        prisma.failed.create({ data: { annotationId: d.annotationId, ...stamp, ...content, premiseFailed: !d.premiseOk, hypothesisFailed: !d.hypothesisOk, note: normalizeNote(d.note) } }),
      ]);
    }
  }

  const after = (await prisma.correct.count({ where: { reviewerId: rid } })) +
    (await prisma.failed.count({ where: { reviewerId: rid } }));
  console.log(`\nafter: ${after} reviewed  (+${after - before})`);
}
main().finally(() => prisma.$disconnect());
