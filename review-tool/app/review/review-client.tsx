"use client";

import Link from "next/link";
import { useCallback, useEffect, useState, useTransition } from "react";
import { getReviewData, submitVerdict } from "./actions";
import type { ReviewData } from "./types";

function toState(d: ReviewData): { p: boolean | null; h: boolean | null; note: string } {
  if (d.verdict) return { p: d.verdict.premiseOk, h: d.verdict.hypothesisOk, note: d.verdict.note ?? "" };
  return { p: null, h: null, note: "" };
}

export function ReviewClient({ initial }: { initial: ReviewData }) {
  const [data, setData] = useState<ReviewData>(initial);
  const init = toState(initial);
  const [premiseOk, setPremiseOk] = useState<boolean | null>(init.p);
  const [hypothesisOk, setHypothesisOk] = useState<boolean | null>(init.h);
  const [note, setNote] = useState<string>(init.note);
  const [pending, startTransition] = useTransition();

  const apply = useCallback((d: ReviewData) => {
    setData(d);
    const s = toState(d);
    setPremiseOk(s.p);
    setHypothesisOk(s.h);
    setNote(s.note);
  }, []);

  const load = useCallback(
    (pos: number) => {
      startTransition(async () => apply(await getReviewData(pos)));
    },
    [apply],
  );

  const save = useCallback(() => {
    if (!data.annotation || premiseOk === null || hypothesisOk === null || pending) return;
    const id = data.annotation.id;
    const p = premiseOk;
    const h = hypothesisOk;
    const nt = note;
    const next = Math.min(data.position + 1, data.total - 1);
    startTransition(async () => {
      await submitVerdict(id, p, h, nt);
      apply(await getReviewData(next));
    });
  }, [data, premiseOk, hypothesisOk, note, pending, apply]);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const t = e.target as HTMLElement | null;
      if (t && (t.tagName === "TEXTAREA" || t.tagName === "INPUT")) return;
      const k = e.key.toLowerCase();
      if (k === "a") setPremiseOk(true);
      else if (k === "s") setPremiseOk(false);
      else if (k === "k") setHypothesisOk(true);
      else if (k === "l") setHypothesisOk(false);
      else if (k === "enter") {
        e.preventDefault();
        save();
      } else if (k === "arrowleft" || k === "[") load(Math.max(0, data.position - 1));
      else if (k === "arrowright" || k === "]") load(Math.min(data.total - 1, data.position + 1));
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [save, load, data.position, data.total]);

  const a = data.annotation;
  if (!a) {
    return (
      <main className="mx-auto max-w-3xl p-8 text-center">
        <h1 className="text-xl font-semibold">Nothing to review 🎉</h1>
        <p className="mt-2 text-sm text-neutral-500">All {data.total} annotations are done.</p>
        <div className="mt-4 flex justify-center gap-4 text-sm underline">
          <Link href="/failed">Failed &amp; fixes</Link>
          <Link href="/dashboard">Dashboard</Link>
        </div>
      </main>
    );
  }

  const pct = data.total ? Math.round((data.progress.reviewed / data.total) * 100) : 0;
  const remaining = data.total - data.progress.reviewed;
  const firstUnreviewed = data.progress.firstUnreviewed;

  return (
    <main className="mx-auto max-w-4xl p-6">
      <div className="mb-4">
        <div className="flex items-center justify-between text-sm text-neutral-500">
          <span>
            Pair {data.position + 1} / {data.total}
          </span>
          <span>
            {data.progress.reviewed} reviewed · {data.progress.correct} ✓ · {data.progress.failed} ✗
          </span>
        </div>
        <div className="mt-1 h-1.5 w-full rounded bg-neutral-200">
          <div className="h-1.5 rounded bg-black transition-all" style={{ width: `${pct}%` }} />
        </div>
        <div className="mt-1.5 flex items-center gap-3 text-xs">
          {firstUnreviewed === null ? (
            <span className="text-green-600">All {data.total} pairs reviewed ✓</span>
          ) : (
            <>
              <span className="text-neutral-500">{remaining} unreviewed left</span>
              {firstUnreviewed !== data.position && (
                <button
                  onClick={() => load(firstUnreviewed)}
                  disabled={pending}
                  className="rounded border border-amber-400 px-2 py-0.5 font-medium text-amber-700 disabled:opacity-40"
                >
                  Jump to first unreviewed (pair {firstUnreviewed + 1})
                </button>
              )}
            </>
          )}
        </div>
      </div>

      <div className="mb-3 flex items-center gap-2 text-xs text-neutral-500">
        <span className="rounded bg-neutral-100 px-2 py-0.5 font-medium uppercase">{a.label}</span>
        <span>#{a.sourceIndex ?? "?"}</span>
        <span>{a.generationModel}</span>
        {data.verdict && <span className="text-amber-600">already reviewed — editing</span>}
        {pending && <span className="text-neutral-400">saving…</span>}
      </div>

      <Panel title="Premise" english={a.premise} metta={a.mettaPremise} ok={premiseOk} setOk={setPremiseOk} yes="A" no="S" />
      <div className="h-4" />
      <Panel title="Hypothesis" english={a.hypothesis} metta={a.mettaHypothesis} ok={hypothesisOk} setOk={setHypothesisOk} yes="K" no="L" />

      <textarea
        value={note}
        onChange={(e) => setNote(e.target.value)}
        placeholder="Optional note (e.g. why it failed)…"
        rows={2}
        className="mt-4 w-full rounded border border-neutral-300 p-2 text-sm"
      />

      <div className="mt-4 flex items-center gap-3">
        <button
          onClick={() => load(Math.max(0, data.position - 1))}
          disabled={pending || data.position === 0}
          className="rounded border px-3 py-1.5 text-sm disabled:opacity-40"
        >
          ← Prev
        </button>
        <button
          onClick={save}
          disabled={pending || premiseOk === null || hypothesisOk === null}
          className="rounded bg-black px-4 py-1.5 text-sm text-white disabled:opacity-40"
        >
          Save &amp; next ⏎
        </button>
        <button
          onClick={() => load(Math.min(data.total - 1, data.position + 1))}
          disabled={pending || data.position >= data.total - 1}
          className="rounded border px-3 py-1.5 text-sm disabled:opacity-40"
        >
          Next →
        </button>
        <div className="ml-auto flex gap-4 text-xs text-neutral-500 underline">
          <Link href="/failed">Failed</Link>
          <Link href="/dashboard">Dashboard</Link>
        </div>
      </div>

      <p className="mt-4 text-xs text-neutral-400">
        Keys: <b>A</b>/<b>S</b> premise yes/no · <b>K</b>/<b>L</b> hypothesis yes/no · <b>Enter</b> save &amp; next · <b>←</b>/<b>→</b> navigate
      </p>
    </main>
  );
}

function Panel({
  title,
  english,
  metta,
  ok,
  setOk,
  yes,
  no,
}: {
  title: string;
  english: string;
  metta: string;
  ok: boolean | null;
  setOk: (v: boolean) => void;
  yes: string;
  no: string;
}) {
  return (
    <section className="rounded-lg border border-neutral-200 p-4">
      <div className="mb-2 flex items-center justify-between gap-2">
        <h2 className="text-sm font-semibold">{title}</h2>
        <div className="flex gap-2">
          <button
            onClick={() => setOk(true)}
            className={`rounded px-3 py-1 text-sm ${ok === true ? "bg-green-600 text-white" : "border"}`}
          >
            ✓ Yes ({yes})
          </button>
          <button
            onClick={() => setOk(false)}
            className={`rounded px-3 py-1 text-sm ${ok === false ? "bg-red-600 text-white" : "border"}`}
          >
            ✗ No ({no})
          </button>
        </div>
      </div>
      <p className="text-sm">{english}</p>
      <pre className="mt-2 overflow-x-auto whitespace-pre-wrap rounded bg-neutral-50 p-2 text-xs">{metta}</pre>
    </section>
  );
}
