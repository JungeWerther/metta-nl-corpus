import { readFileSync } from "node:fs";
import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaClient } from "../lib/generated/prisma/client";

// Read the Neon (production) URL straight from .env.local, ignoring any
// stale DATABASE_URL in the shell / .env (which points at a local dev DB).
function dbUrl(): string {
  if (process.env.DIAG_DATABASE_URL) return process.env.DIAG_DATABASE_URL;
  const line = readFileSync(".env.local", "utf8")
    .split("\n")
    .find((l) => l.startsWith("DATABASE_URL="));
  if (!line) throw new Error("DATABASE_URL not found in .env.local");
  return line.slice("DATABASE_URL=".length).trim().replace(/^["']|["']$/g, "");
}

const url = dbUrl();
console.error("using host:", url.replace(/.*@([^/]+)\/.*/, "$1"));
const prisma = new PrismaClient({ adapter: new PrismaPg({ connectionString: url }) });

const ORDER = [{ sourceIndex: "asc" as const }, { id: "asc" as const }];

async function main() {
  const anns = await prisma.annotation.findMany({ orderBy: ORDER, select: { id: true } });
  const idx = new Map<string, number>();
  anns.forEach((a, i) => idx.set(a.id, i));
  const total = anns.length;

  const [corrects, faileds] = await Promise.all([
    prisma.correct.findMany({ select: { annotationId: true, reviewerId: true } }),
    prisma.failed.findMany({ select: { annotationId: true, reviewerId: true } }),
  ]);

  const byReviewer = new Map<string, Set<number>>();
  const add = (rid: string, aid: string) => {
    const p = idx.get(aid);
    if (p == null) return;
    if (!byReviewer.has(rid)) byReviewer.set(rid, new Set());
    byReviewer.get(rid)!.add(p);
  };
  corrects.forEach((c) => add(c.reviewerId, c.annotationId));
  faileds.forEach((f) => add(f.reviewerId, f.annotationId));

  console.log(`total annotations: ${total}\n`);
  for (const [rid, set] of byReviewer) {
    const positions = [...set].sort((a, b) => a - b);
    const reviewed = positions.length;
    const maxPos = positions[positions.length - 1];
    const resume = Math.min(reviewed, total - 1); // what the tool jumps to on reload
    // holes = unreviewed positions BEFORE the last reviewed one
    const holes: number[] = [];
    for (let i = 0; i <= maxPos; i++) if (!set.has(i)) holes.push(i);
    console.log(`reviewer ${rid.slice(0, 12)}…`);
    console.log(`  reviewed:        ${reviewed}`);
    console.log(`  last reviewed:   pair ${maxPos + 1} (index ${maxPos})`);
    console.log(`  reload resumes:  pair ${resume + 1} (index ${resume}) — already reviewed? ${set.has(resume)}`);
    console.log(`  holes (unreviewed before last): ${holes.length}`);
    if (holes.length) console.log(`    -> pairs: ${holes.slice(0, 20).map((h) => h + 1).join(", ")}${holes.length > 20 ? " …" : ""}`);
    console.log("");
  }
}

main().finally(() => prisma.$disconnect());
