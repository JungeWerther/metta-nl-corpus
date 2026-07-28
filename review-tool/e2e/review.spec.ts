import { expect, test } from "@playwright/test";

test("review a pair, see progress update, then reach export", async ({ page }) => {
  await page.goto("/review");

  // A pair renders (both panels + keyboard hints).
  await expect(page.getByRole("heading", { name: "Premise" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Hypothesis" })).toBeVisible();

  // Mark premise Yes (A) and hypothesis Yes (K), then Save & next.
  await page.getByRole("button", { name: /Yes \(A\)/ }).click();
  await page.getByRole("button", { name: /Yes \(K\)/ }).click();
  await page.getByRole("button", { name: /Save & next/ }).click();

  // Progress reflects one review.
  await expect(page.getByText(/1 reviewed/)).toBeVisible();
});

test("export page lists tables with download links", async ({ page }) => {
  await page.goto("/export");
  await expect(page.getByRole("heading", { name: "Export" })).toBeVisible();
  await expect(page.getByText(/Final — gold/)).toBeVisible();
  // download links exist for JSON/CSV
  await expect(page.getByRole("link", { name: "JSON" }).first()).toBeVisible();
  await expect(page.getByRole("link", { name: "CSV" }).first()).toBeVisible();
});

test("dashboard renders", async ({ page }) => {
  await page.goto("/dashboard");
  await expect(page.getByRole("heading", { name: "Dashboard" })).toBeVisible();
});
