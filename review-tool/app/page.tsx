import { redirect } from "next/navigation";

// No auth — go straight to the review screen.
export default function Home() {
  redirect("/review");
}
