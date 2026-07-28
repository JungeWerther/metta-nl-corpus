import { getStats } from "./actions";
import { DashboardClient } from "./dashboard-client";

export const dynamic = "force-dynamic";

export default async function DashboardPage() {
  const stats = await getStats();
  return <DashboardClient stats={stats} />;
}
