"use client";

import Link from "next/link";
import { useState, useTransition } from "react";
import {
  Bar,
  BarChart,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { getStats } from "./actions";
import type { Stats } from "./types";

const GREEN = "#16a34a";
const RED = "#dc2626";
const BLUE = "#2563eb";
const AMBER = "#d97706";

type Tone = "neutral" | "green" | "red" | "amber" | "blue";

export function DashboardClient({ stats: initial }: { stats: Stats }) {
  const [stats, setStats] = useState<Stats>(initial);
  const [pending, start] = useTransition();
  const refresh = () => start(async () => setStats(await getStats()));

  const verdictPie = [
    { name: "Correct", value: stats.correct },
    { name: "Failed", value: stats.failed },
  ];
  const sideBars = [
    { name: "Premise", Yes: stats.premiseYes, No: stats.premiseNo },
    { name: "Hypothesis", Yes: stats.hypothesisYes, No: stats.hypothesisNo },
  ];
  const goldPie = [
    { name: "Direct", value: stats.goldDirect },
    { name: "Fixed", value: stats.goldFixed },
  ];
  const reviewedPct = stats.total ? Math.round((stats.reviewed / stats.total) * 100) : 0;

  return (
    <main className="mx-auto max-w-5xl p-6">
      <header className="mb-6 flex items-center gap-3">
        <h1 className="text-xl font-semibold">Dashboard</h1>
        <button
          onClick={refresh}
          disabled={pending}
          className="rounded border px-3 py-1 text-sm disabled:opacity-40"
        >
          {pending ? "…" : "Refresh"}
        </button>
        <div className="ml-auto flex gap-4 text-sm underline">
          <Link href="/review">Review</Link>
          <Link href="/failed">Failed</Link>
        </div>
      </header>

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 md:grid-cols-6">
        <Card label="Total" value={stats.total} />
        <Card label="Reviewed" value={`${stats.reviewed} · ${reviewedPct}%`} />
        <Card label="Correct" value={stats.correct} tone="green" />
        <Card label="Failed" value={stats.failed} tone="red" />
        <Card label="To fix" value={stats.toFix} tone="amber" />
        <Card label="Gold" value={stats.gold} tone="blue" />
      </div>

      {stats.reviewed === 0 ? (
        <p className="mt-8 text-sm text-neutral-500">No reviews yet — start on the Review page.</p>
      ) : (
        <div className="mt-8 grid gap-6 md:grid-cols-2">
          <ChartCard title="Correct vs Failed">
            <ResponsiveContainer width="100%" height={260}>
              <PieChart>
                <Pie data={verdictPie} dataKey="value" nameKey="name" outerRadius={90} label>
                  <Cell fill={GREEN} />
                  <Cell fill={RED} />
                </Pie>
                <Tooltip />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </ChartCard>

          <ChartCard title="Faithful by side (Yes vs No)">
            <ResponsiveContainer width="100%" height={260}>
              <BarChart data={sideBars}>
                <XAxis dataKey="name" />
                <YAxis allowDecimals={false} />
                <Tooltip />
                <Legend />
                <Bar dataKey="Yes" fill={GREEN} />
                <Bar dataKey="No" fill={RED} />
              </BarChart>
            </ResponsiveContainer>
          </ChartCard>

          {stats.gold > 0 && (
            <ChartCard title="Gold composition (Direct vs Fixed)">
              <ResponsiveContainer width="100%" height={260}>
                <PieChart>
                  <Pie data={goldPie} dataKey="value" nameKey="name" outerRadius={90} label>
                    <Cell fill={BLUE} />
                    <Cell fill={AMBER} />
                  </Pie>
                  <Tooltip />
                  <Legend />
                </PieChart>
              </ResponsiveContainer>
            </ChartCard>
          )}
        </div>
      )}
    </main>
  );
}

function Card({ label, value, tone = "neutral" }: { label: string; value: number | string; tone?: Tone }) {
  const color: Record<Tone, string> = {
    neutral: "text-neutral-900",
    green: "text-green-700",
    red: "text-red-700",
    amber: "text-amber-700",
    blue: "text-blue-700",
  };
  return (
    <div className="rounded-lg border border-neutral-200 p-3">
      <div className="text-xs text-neutral-500">{label}</div>
      <div className={`mt-1 text-xl font-semibold ${color[tone]}`}>{value}</div>
    </div>
  );
}

function ChartCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-neutral-200 p-4">
      <h2 className="mb-2 text-sm font-semibold">{title}</h2>
      {children}
    </div>
  );
}
