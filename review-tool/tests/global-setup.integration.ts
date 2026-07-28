import { execFileSync } from "node:child_process";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// Runs once before the integration suite. Creates the schema on the TEST DB.
// --url targets TEST_DATABASE_URL explicitly, never the real DB from .env.local.
// Retries because a suspended Neon branch can reject the first connection (P1001)
// while its compute wakes up.
export default async function setup() {
  const url = process.env.TEST_DATABASE_URL;
  if (!url) {
    console.warn(
      "\n[integration] TEST_DATABASE_URL not set — integration tests will be SKIPPED.\n" +
        "  Point it at a throwaway Postgres (e.g. a Neon branch) to run them:\n" +
        "  TEST_DATABASE_URL=postgresql://... npm run test:integration\n",
    );
    return;
  }

  const attempts = 4;
  for (let i = 1; i <= attempts; i++) {
    try {
      execFileSync("npx", ["prisma", "db", "push", "--url", url, "--accept-data-loss"], {
        stdio: "inherit",
      });
      return;
    } catch (e) {
      if (i === attempts) throw e;
      console.warn(
        `[integration] schema push attempt ${i}/${attempts} failed ` +
          "(Neon branch may be waking from sleep) — retrying in 6s...",
      );
      await sleep(6000);
    }
  }
}
