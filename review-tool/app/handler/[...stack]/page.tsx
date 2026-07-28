import { StackHandler } from "@hexclave/next";
import { stackServerApp } from "@/stack/server";

// Serves Stack Auth's sign-in / sign-up / account pages under /handler/*.
export default function Handler(props: unknown) {
  return <StackHandler fullPage app={stackServerApp} routeProps={props} />;
}
