// Resolves the current reviewer (id + display name) used to stamp verdicts.
// Production: the authenticated Hexclave user. Tests: a fixed value via
// REVIEW_TEST_USER (never honored in production) so the suite runs without auth.

export type Reviewer = { id: string; name: string };

export async function getReviewer(): Promise<Reviewer> {
  // Auth bypass for tests only. Honored under Vitest, OR for E2E when the
  // explicit E2E_AUTH_BYPASS flag is set — but NEVER in a production build
  // (NODE_ENV === "production" can never bypass, regardless of flags).
  const inVitest = !!process.env.VITEST || process.env.NODE_ENV === "test";
  const e2eBypass =
    process.env.NODE_ENV !== "production" && process.env.E2E_AUTH_BYPASS === "1";
  if ((inVitest || e2eBypass) && process.env.REVIEW_TEST_USER) {
    return { id: process.env.REVIEW_TEST_USER, name: process.env.REVIEW_TEST_USER };
  }

  // Lazy imports so tests never construct the auth app / touch server-only code.
  const { stackServerApp } = await import("@/stack/server");
  const user = await stackServerApp.getUser();
  if (!user) {
    const { redirect } = await import("next/navigation");
    redirect("/handler/sign-in");
  }
  const id = user?.id ?? "unknown";
  const name = user?.displayName ?? user?.primaryEmail ?? id;
  return { id, name };
}
