import { resolve } from "node:path";
import { defineConfig } from "vitest/config";

// Unit tests: pure logic + validation. No database, always runnable.
export default defineConfig({
  resolve: {
    alias: { "@": resolve(import.meta.dirname, ".") },
  },
  test: {
    include: ["tests/unit/**/*.test.ts"],
    environment: "node",
  },
});
