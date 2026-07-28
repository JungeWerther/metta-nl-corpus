import "server-only";
import { StackServerApp } from "@hexclave/next";

// Reads NEXT_PUBLIC_STACK_PROJECT_ID / NEXT_PUBLIC_STACK_PUBLISHABLE_CLIENT_KEY /
// STACK_SECRET_SERVER_KEY from the environment.
export const stackServerApp = new StackServerApp({
  tokenStore: "nextjs-cookie",
  urls: {
    afterSignIn: "/review",
    afterSignUp: "/review",
    afterSignOut: "/handler/sign-in",
  },
});
