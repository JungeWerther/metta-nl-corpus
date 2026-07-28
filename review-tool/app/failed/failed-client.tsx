"use client";

import Link from "next/link";
import { useState, useTransition } from "react";
import { getFailedList, revertFix, submitFix } from "./actions";
import type { FailedItem } from "./types";

export function FailedClient({ initial }: { initial: FailedItem[] }) {
  const [items, setItems] = useState<FailedItem[]>(initial);
  const [showFixed, setShowFixed] = useState(true);
  const [openId, setOpenId] = useState<string | null>(null);
  const [, startRefresh] = useTransition();

  const refresh = () => startRefresh(async () => setItems(await getFailedList()));

  const unfixed = items.filter((i) => !i.isFixed).length;
  const visible = items.filter((i) => showFixed || !i.isFixed);

  return (
    <main className="mx-auto max-w-4xl p-6">
      <header className="mb-4 flex flex-wrap items-center gap-3">
        <h1 className="text-xl font-semibold">Failed pairs</h1>
        <span className="text-sm text-neutral-500">
          {items.length} total · {unfixed} to fix · {items.length - unfixed} fixed
        </span>
        <label className="ml-auto flex items-center gap-1 text-sm text-neutral-600">
          <input type="checkbox" checked={showFixed} onChange={(e) => setShowFixed(e.target.checked)} />
          show fixed
        </label>
        <Link href="/review" className="text-sm underline">
          Review
        </Link>
        <Link href="/dashboard" className="text-sm underline">
          Dashboard
        </Link>
      </header>

      {visible.length === 0 ? (
        <p className="text-sm text-neutral-500">Nothing here 🎉</p>
      ) : (
        <ul className="space-y-3">
          {visible.map((item) => (
            <FailedRow
              key={item.id}
              item={item}
              open={openId === item.id}
              onToggle={() => setOpenId(openId === item.id ? null : item.id)}
              onChanged={refresh}
            />
          ))}
        </ul>
      )}
    </main>
  );
}

function Tag({ children, tone = "neutral" }: { children: React.ReactNode; tone?: "neutral" | "red" | "green" | "amber" }) {
  const cls = {
    neutral: "bg-neutral-100 text-neutral-600",
    red: "bg-red-100 text-red-700",
    green: "bg-green-100 text-green-700",
    amber: "bg-amber-100 text-amber-700",
  }[tone];
  return <span className={`rounded px-2 py-0.5 text-xs font-medium ${cls}`}>{children}</span>;
}

function FailedRow({
  item,
  open,
  onToggle,
  onChanged,
}: {
  item: FailedItem;
  open: boolean;
  onToggle: () => void;
  onChanged: () => void;
}) {
  const [fp, setFp] = useState(item.fixed?.fixedMettaPremise ?? item.mettaPremise);
  const [fh, setFh] = useState(item.fixed?.fixedMettaHypothesis ?? item.mettaHypothesis);
  const [fn, setFn] = useState(item.fixed?.note ?? "");
  const [err, setErr] = useState<string | null>(null);
  const [pending, start] = useTransition();

  const save = () =>
    start(async () => {
      const res = await submitFix(item.id, fp, fh, fn);
      if ("error" in res) setErr(res.error);
      else {
        setErr(null);
        onChanged();
      }
    });

  const revert = () =>
    start(async () => {
      await revertFix(item.id);
      onChanged();
    });

  return (
    <li className="rounded-lg border border-neutral-200 p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="mb-1 flex flex-wrap gap-1.5">
            <Tag>{item.label}</Tag>
            {item.premiseFailed && <Tag tone="red">premise ✗</Tag>}
            {item.hypothesisFailed && <Tag tone="red">hypothesis ✗</Tag>}
            {item.isFixed ? <Tag tone="green">fixed</Tag> : <Tag tone="amber">to fix</Tag>}
          </div>
          <p className="text-sm">
            <span className="text-neutral-400">P:</span> {item.premise}
          </p>
          <p className="text-sm">
            <span className="text-neutral-400">H:</span> {item.hypothesis}
          </p>
          {item.note && <p className="mt-1 text-xs text-neutral-500">note: {item.note}</p>}
        </div>
        <button onClick={onToggle} className="shrink-0 rounded border px-3 py-1 text-sm">
          {open ? "Close" : "Fix"}
        </button>
      </div>

      {open && (
        <div className="mt-4 grid gap-3 border-t border-neutral-100 pt-4">
          <div className="grid gap-3 md:grid-cols-2">
            <div>
              <div className="mb-1 text-xs font-medium text-neutral-500">Original premise MeTTa</div>
              <pre className="overflow-x-auto whitespace-pre-wrap rounded bg-neutral-50 p-2 text-xs">{item.mettaPremise}</pre>
            </div>
            <div>
              <div className="mb-1 text-xs font-medium text-neutral-500">Original hypothesis MeTTa</div>
              <pre className="overflow-x-auto whitespace-pre-wrap rounded bg-neutral-50 p-2 text-xs">{item.mettaHypothesis}</pre>
            </div>
          </div>

          <label className="block">
            <span className="text-xs font-medium">
              Fixed premise MeTTa {item.premiseFailed && <span className="text-red-600">(failed)</span>}
            </span>
            <textarea
              value={fp}
              onChange={(e) => setFp(e.target.value)}
              rows={3}
              className="mt-1 w-full rounded border border-neutral-300 p-2 font-mono text-xs"
            />
          </label>
          <label className="block">
            <span className="text-xs font-medium">
              Fixed hypothesis MeTTa {item.hypothesisFailed && <span className="text-red-600">(failed)</span>}
            </span>
            <textarea
              value={fh}
              onChange={(e) => setFh(e.target.value)}
              rows={3}
              className="mt-1 w-full rounded border border-neutral-300 p-2 font-mono text-xs"
            />
          </label>
          <label className="block">
            <span className="text-xs font-medium">Note (why / how you fixed it)</span>
            <textarea
              value={fn}
              onChange={(e) => setFn(e.target.value)}
              rows={2}
              placeholder="Optional…"
              className="mt-1 w-full rounded border border-neutral-300 p-2 text-xs"
            />
          </label>

          {err && <p className="text-xs text-red-600">{err}</p>}

          <div className="flex items-center gap-2">
            <button
              onClick={save}
              disabled={pending}
              className="rounded bg-black px-4 py-1.5 text-sm text-white disabled:opacity-40"
            >
              {item.isFixed ? "Update fix" : "Save fix"}
            </button>
            {item.isFixed && (
              <button
                onClick={revert}
                disabled={pending}
                className="rounded border px-3 py-1.5 text-sm disabled:opacity-40"
              >
                Revert fix
              </button>
            )}
            {pending && <span className="text-xs text-neutral-400">saving…</span>}
          </div>
        </div>
      )}
    </li>
  );
}
