"use client";

import { useEffect, useRef } from "react";

import { describeYear, formatWithUnit } from "@/lib/format";
import {
  absenceReason,
  groupByFamily,
  nearestYear,
  rowFor,
  type SeriesIndex,
} from "@/lib/series";
import { ordinal, rankOf } from "@/lib/stats";
import type { BoroughRef, MetricCoverage } from "@/lib/types";

/**
 * Borough detail (plan issue 3.5).
 *
 * Every metric for one borough, with three things the acceptance criteria call
 * out and that a naive implementation gets wrong:
 *
 *   The rank denominator is the metric's own coverage. Well-being covers 32
 *   boroughs, so its ranks are n/32. Printing n/33 would assert a position for
 *   City of London that the source refuses to estimate.
 *
 *   An empty cell says WHY it is empty. "Not published for City of London",
 *   "IMD exists only for 2015 and 2019", and "no value published for Camden in
 *   2019" are three different facts, and a blank cell collapses them into one
 *   unhelpful one.
 *
 *   A metric whose series does not reach the selected year shows its nearest
 *   published year, labelled as such. Showing nothing for every IMD domain
 *   because the reader is looking at 2023 would be technically correct and
 *   practically useless; silently showing the 2019 value as though it were 2023
 *   would be worse.
 *
 * It is a panel, not a modal. A modal needs a focus trap and an inert
 * background, and buys nothing here — the map underneath stays useful, and at
 * 375 px the panel is simply the next thing down the page.
 */
export function BoroughDetail({
  borough,
  boroughIndex,
  boroughs,
  metrics,
  series,
  year,
  onClose,
}: {
  borough: BoroughRef;
  boroughIndex: number;
  boroughs: readonly BoroughRef[];
  metrics: Record<string, MetricCoverage>;
  series: SeriesIndex;
  year: number;
  onClose: () => void;
}) {
  const headingRef = useRef<HTMLHeadingElement | null>(null);

  useEffect(() => {
    headingRef.current?.focus();
  }, [borough.gss]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);

  const groups = groupByFamily(metrics);

  return (
    <section
      aria-labelledby="borough-detail-heading"
      className="rounded-lg border border-[var(--border)] bg-[var(--surface-1)] p-4 sm:p-5"
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2
            id="borough-detail-heading"
            ref={headingRef}
            tabIndex={-1}
            className="text-base font-semibold text-[var(--text-primary)]"
          >
            {borough.name}
          </h2>
          <p className="text-xs text-[var(--text-secondary)]">
            All metrics near <span className="tabular-nums">{year}</span> · ranks are against each
            metric&apos;s own borough coverage
          </p>
        </div>
        <button
          type="button"
          onClick={onClose}
          className="min-h-11 min-w-11 shrink-0 rounded-md border border-[var(--border)] px-3 text-sm text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
        >
          Close
        </button>
      </div>

      <div className="mt-3 space-y-4">
        {groups.map((group) => (
          <div key={group.family}>
            <h3 className="text-xs font-semibold uppercase tracking-wide text-[var(--text-secondary)]">
              {group.family}
            </h3>
            <dl className="mt-1 divide-y divide-[var(--border)]">
              {group.ids.map((id) => (
                <MetricRow
                  key={id}
                  metric={metrics[id]}
                  metricId={id}
                  series={series}
                  year={year}
                  borough={borough}
                  boroughIndex={boroughIndex}
                  boroughCount={boroughs.length}
                />
              ))}
            </dl>
          </div>
        ))}
      </div>
    </section>
  );
}

function MetricRow({
  metric,
  metricId,
  series,
  year,
  borough,
  boroughIndex,
  boroughCount,
}: {
  metric: MetricCoverage;
  metricId: string;
  series: SeriesIndex;
  year: number;
  borough: BoroughRef;
  boroughIndex: number;
  boroughCount: number;
}) {
  // Use the selected year where the metric has it; otherwise its nearest
  // published year, said out loud rather than substituted quietly.
  const exact = metric.years.includes(year);
  const usedYear = exact ? year : nearestYear(metric, year);

  const row = usedYear === null ? [] : rowFor(series, metricId, usedYear, boroughCount);
  const value = usedYear === null ? null : (row[boroughIndex] ?? null);
  const rank = value === null ? null : rankOf(row, boroughIndex, metric.direction);

  return (
    <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-0.5 py-1.5">
      <dt className="text-sm text-[var(--text-secondary)]">
        {metric.label}
        {!exact && usedYear !== null && (
          <span className="ml-1 text-xs">
            · <span className="tabular-nums">{describeYear(usedYear, metric)}</span>, its nearest
            published year
          </span>
        )}
      </dt>
      <dd className="text-right text-sm">
        {value === null ? (
          <span className="text-xs text-[var(--text-secondary)]">
            {absenceReason(metric, metric.label, usedYear ?? year, borough).text}
          </span>
        ) : (
          <>
            <span className="font-medium tabular-nums text-[var(--text-primary)]">
              {formatWithUnit(value, metric)}
            </span>
            {rank && (
              <span className="ml-2 text-xs tabular-nums text-[var(--text-secondary)]">
                {ordinal(rank.rank)} of {rank.of}
                {rank.tied > 1 && ` (tied with ${rank.tied - 1})`}
                {rank.worstFirst && <span className="sr-only"> — 1 is the worse end</span>}
              </span>
            )}
          </>
        )}
      </dd>
    </div>
  );
}
