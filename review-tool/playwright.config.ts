import { defineConfig } from "@playwright/test";

// E2E runs against a dev server in TEST-MODE (auth bypassed via E2E_AUTH_BYPASS),
// pointed at the THROWAWAY TEST_DATABASE_URL (never the main DB).
const TEST_DB = process.env.TEST_DATABASE_URL ?? "";
const PORT = 3100;

export default defineConfig({
  testDir: "./e2e",
  globalSetup: "./e2e/global-setup.ts",
  timeout: 60_000,
  expect: { timeout: 15_000 },
  fullyParallel: false,
  workers: 1,
  use: { baseURL: `http://localhost:${PORT}` },
  webServer: {
    command: `npm run dev -- --port ${PORT}`,
    url: `http://localhost:${PORT}/review`,
    reuseExistingServer: false,
    timeout: 180_000,
    env: {
      E2E_AUTH_BYPASS: "1",
      REVIEW_TEST_USER: "e2e-user",
      // Override the app's DB with the throwaway branch (set before Next boots,
      // so .env.local's DATABASE_URL does not take effect).
      DATABASE_URL: TEST_DB,
    },
  },
});
