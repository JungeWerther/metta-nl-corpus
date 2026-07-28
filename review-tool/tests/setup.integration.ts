// Runs before each integration test file, BEFORE the app modules (and the
// Prisma singleton) are imported — so the singleton connects to the test DB.
if (process.env.TEST_DATABASE_URL) {
  process.env.DATABASE_URL = process.env.TEST_DATABASE_URL;
}
// Reviewer id used by the actions when there is no auth session (tests only).
process.env.REVIEW_TEST_USER = "test-reviewer";
