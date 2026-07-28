import { prisma } from "@/lib/prisma";
import { getReviewer } from "@/lib/reviewer";

export const dynamic = "force-dynamic";

export default async function ExportPage() {
  const { id: rid } = await getReviewer();
  const [final, correct, failed, fixed] = await Promise.all([
    prisma.final.count({ where: { reviewerId: rid } }),
    prisma.correct.count({ where: { reviewerId: rid } }),
    prisma.failed.count({ where: { reviewerId: rid } }),
    prisma.fixed.count({ where: { reviewerId: rid } }),
  ]);

  const rows = [
    { table: "final", label: "Final — gold (training set)", count: final },
    { table: "correct", label: "Correct", count: correct },
    { table: "failed", label: "Failed", count: failed },
    { table: "fixed", label: "Fixed", count: fixed },
  ];

  return (
    <main className="mx-auto max-w-3xl p-6">
      <h1 className="text-xl font-semibold">Export</h1>
      <p className="mt-1 text-sm text-neutral-500">
        Download your reviewed data to your device. JSON for training, CSV for spreadsheets.
      </p>
      <div className="mt-4 divide-y divide-neutral-200 rounded-lg border border-neutral-200">
        {rows.map((r) => (
          <div key={r.table} className="flex items-center justify-between p-4">
            <div>
              <div className="text-sm font-medium">{r.label}</div>
              <div className="text-xs text-neutral-500">{r.count} rows</div>
            </div>
            <div className="flex gap-2">
              <a
                href={`/api/export?table=${r.table}&format=json`}
                className="rounded border border-neutral-300 px-3 py-1 text-sm hover:bg-neutral-50"
              >
                JSON
              </a>
              <a
                href={`/api/export?table=${r.table}&format=csv`}
                className="rounded border border-neutral-300 px-3 py-1 text-sm hover:bg-neutral-50"
              >
                CSV
              </a>
            </div>
          </div>
        ))}
      </div>
      <p className="mt-4 text-xs text-neutral-400">
        Exports are scoped to your reviews. (An all-reviewers export can be added later.)
      </p>
    </main>
  );
}
