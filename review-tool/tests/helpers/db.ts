import { randomUUID } from "node:crypto";
import { prisma } from "@/lib/prisma";

export const hasTestDb = !!process.env.TEST_DATABASE_URL;

/** Wipe every table (FK-safe) between tests. */
export async function truncateAll() {
  await prisma.$executeRawUnsafe(
    'TRUNCATE "Fixed","Final","Correct","Failed","Annotation" RESTART IDENTITY CASCADE;',
  );
}

type AnnotationSeed = {
  id?: string;
  premise?: string;
  hypothesis?: string;
  label?: string;
  mettaPremise?: string;
  mettaHypothesis?: string;
  sourceIndex?: number;
  generationModel?: string;
};

/** Insert one Annotation with sensible defaults; returns its id. */
export async function seedAnnotation(over: AnnotationSeed = {}): Promise<string> {
  const id = over.id ?? randomUUID();
  await prisma.annotation.create({
    data: {
      id,
      premise: over.premise ?? "A dog runs.",
      hypothesis: over.hypothesis ?? "An animal moves.",
      label: over.label ?? "entailment",
      mettaPremise: over.mettaPremise ?? "(dog a-dog) (runs a-dog)",
      mettaHypothesis: over.mettaHypothesis ?? "(animal a-dog) (moves a-dog)",
      sourceIndex: over.sourceIndex ?? 0,
      generationModel: over.generationModel ?? "test-model",
    },
  });
  return id;
}

export { prisma };
