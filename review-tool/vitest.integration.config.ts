import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { defineConfig } from "vitest/config";

// Integration tests: real server actions against a real Postgres.
// Point TEST_DATABASE_URL at a THROWAWAY database (e.g. a Neon branch).
// Tests skip cleanly when TEST_DATABASE_URL is unset.
//
// Convenience: pick up TEST_DATABASE_URL from git-ignored .env.test.local so
// `npm run test:integration` works without exporting it each run. An explicit
// env var (e.g. in CI) still wins.
if (!process.env.TEST_DATABASE_URL) {
  try {
    const line = readFileSync(resolve(import.meta.dirname, ".env.test.local"), "utf8")
      .split("\n")
      .find((l) => l.trim().startsWith("TEST_DATABASE_URL="));
    if (line) {
      process.env.TEST_DATABASE_URL = line
        .slice(line.indexOf("=") + 1)
        .trim()
        .replace(/^["']|["']$/g, "");
    }
  } catch {
    // no .env.test.local — tests will skip, which is the documented behavior
  }
}
export default defineConfig({
  resolve: {
    alias: { "@": resolve(import.meta.dirname, ".") },
  },
  test: {
    include: ["tests/integration/**/*.test.ts"],
    environment: "node",
    globalSetup: ["tests/global-setup.integration.ts"],
    setupFiles: ["tests/setup.integration.ts"],
    fileParallelism: false, // serialize DB tests — they share one database
    testTimeout: 30_000,
    hookTimeout: 60_000,
  },
});
