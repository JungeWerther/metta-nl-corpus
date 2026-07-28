import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { Pool } from "pg";

// Prepares the THROWAWAY test DB for the E2E run: pushes schema, wipes it,
// seeds a handful of annotations. Uses the raw pg driver (no Prisma import,
// which would pull in an ESM `import.meta` module Playwright can't load).
// Refuses to run without TEST_DATABASE_URL.
export default async function globalSetup() {
  const url = process.env.TEST_DATABASE_URL;
  if (!url) {
    throw new Error("E2E requires TEST_DATABASE_URL pointed at a throwaway DB (never main).");
  }

  execFileSync("npx", ["prisma", "db", "push", "--url", url, "--accept-data-loss"], {
    stdio: "inherit",
  });

  const pool = new Pool({ connectionString: url });
  try {
    await pool.query(
      'TRUNCATE "Fixed","Final","Correct","Failed","Annotation" RESTART IDENTITY CASCADE;',
    );
    for (let i = 0; i < 5; i++) {
      await pool.query(
        'INSERT INTO "Annotation" (id, premise, hypothesis, label, "mettaPremise", "mettaHypothesis", "sourceIndex") ' +
          "VALUES ($1, $2, $3, $4, $5, $6, $7)",
        [
          randomUUID(),
          `E2E premise ${i}`,
          `E2E hypothesis ${i}`,
          "entailment",
          "(dog a-dog) (runs a-dog)",
          "(animal a-dog)",
          i,
        ],
      );
    }
  } finally {
    await pool.end();
  }
}
