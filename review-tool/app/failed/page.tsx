import { getFailedList } from "./actions";
import { FailedClient } from "./failed-client";

export const dynamic = "force-dynamic";

export default async function FailedPage() {
  const items = await getFailedList();
  return <FailedClient initial={items} />;
}
