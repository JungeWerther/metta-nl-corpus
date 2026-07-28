"use client";

import { useUser } from "@hexclave/next";

// Minimal header menu: shows the signed-in reviewer's name + a Sign out button.
// (Replaces Hexclave's full UserButton / account-settings menu.)
export function UserMenu() {
  const user = useUser();
  if (!user) return null;
  const name = user.displayName ?? user.primaryEmail ?? "Account";
  return (
    <div className="flex items-center gap-3 text-sm">
      <span className="text-neutral-600">{name}</span>
      <button
        onClick={() => user.signOut()}
        className="rounded border border-neutral-300 px-2.5 py-1 hover:bg-neutral-50"
      >
        Sign out
      </button>
    </div>
  );
}
