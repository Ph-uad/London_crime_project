"use client";

import { describeYear, formatDelta, formatValue, formatWithUnit } from "@/lib/format";
import { rowFor, type SeriesIndex } from "@/lib/series";
import { longRunChange, summarise, worstEnd } from "@/lib/stats";
import type { BoroughRef, MetricCoverage } from "@/lib/types";

/**
 * The KPI strip (plan issue 3.7).
 *
 * Two rules from the coverage matrix decide what these cards are allowed to say:
 *
 *   Extremes are labelled by MEANING, not by value. "Highest" is the wrong word
 *   for the top of a `higher_is_better` metric and a league table nobody wants
 *   to win at the top of a `higher_is_worse` one. The cards read "Most crime"
 *   and "Lowest income" — both of which are the same card, the worse end — so a
 *   reader is never invited to congratulate the borough with the most burglaries.
 *
 *   Trend arrows come from `direction`. Falling crime is an improvement and
 *   falling life expectancy is not, and no amount of looking at the numbers will
 *   tell you which is which. A `neutral` metric gets no judgement at all.
 *
 * The arrow is never the only signal: each card carries the word as well, so the
 * meaning survives a monochrome print, a deuteranope, and a screen reader.
 */
export function KpiPanel({
  boroughs,
  metric,
  metricId,
  series,
  year,
  excluded,
}: {
  boroughs: readonly BoroughRef[];
  metric: MetricCoverage;
  metricId: string;
  series: SeriesIndex;
  year: number;
  excluded: ReadonlySet<string>;
}) {
  const included = (row: readonly (number | null)[]) =>
    row.map((v, i) => (excluded.has(boroughs[i].gss) ? null : v));

  const row = included(rowFor(series, metricId, year, boroughs.length));
  const stats = summarise(row);
  const bad = worstEnd(metric.direction);

  // The London figure is an unweighted mean of boroughs, not a population-
  // weighted rate. Those are different quantities and the label says which.
  const change = longRunChange(metric, (y) => {
    const s = summarise(included(rowFor(series, metricId, y, boroughs.length)));
    return s ? s.mean : null;
  });

  const worst = bad === null ? null : bad === "max" ? stats?.max : stats?.min;
  const best = bad === null ? null : bad === "max" ? stats?.min : stats?.max;

  const cards: { label: string; value: string; sub: string }[] = [];

  // Every borough on the same value. Naming one of them as the "highest" and
  // the same one as the "lowest" is technically true and reads as a bug — which
  // is what happens with imd_employment_score, published to one decimal place
  // and identical across all 33 boroughs.
  const flat = stats !== null && stats.min.value === stats.max.value;

  if (stats && flat) {
    cards.push({
      label: "No variation between boroughs",
      value: formatWithUnit(stats.max.value, metric),
      sub: `identical across all ${stats.n} boroughs at the precision the source publishes`,
    });
  } else if (stats && bad !== null && worst && best) {
    cards.push({
      label: metric.direction === "higher_is_worse" ? "Highest — the worse end" : "Lowest — the worse end",
      value: boroughs[worst.index].name,
      sub: formatWithUnit(worst.value, metric),
    });
    cards.push({
      label: metric.direction === "higher_is_worse" ? "Lowest — the better end" : "Highest — the better end",
      value: boroughs[best.index].name,
      sub: formatWithUnit(best.value, metric),
    });
  } else if (stats) {
    // A neutral metric has no better end, so neither extreme is praised.
    cards.push({
      label: "Highest",
      value: boroughs[stats.max.index].name,
      sub: formatWithUnit(stats.max.value, metric),
    });
    cards.push({
      label: "Lowest",
      value: boroughs[stats.min.index].name,
      sub: formatWithUnit(stats.min.value, metric),
    });
  }

  if (stats) {
    cards.push({
      label: "Borough mean",
      value: formatValue(stats.mean, metric.scale),
      sub: `unweighted across ${stats.n} boroughs, not a population-weighted London figure`,
    });
  }

  return (
    <section aria-labelledby="kpi-heading">
      <h2 id="kpi-heading" className="sr-only">
        Summary for {metric.label}, {describeYear(year, metric)}
      </h2>
      <dl className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        {cards.map((c) => (
          <div
            key={c.label}
            className="rounded-lg border border-[var(--border)] bg-[var(--surface-1)] p-4"
          >
            <dt className="text-xs text-[var(--text-secondary)]">{c.label}</dt>
            <dd className="mt-1">
              <span className="block text-lg font-semibold text-[var(--text-primary)]">
                {c.value}
              </span>
              <span className="mt-0.5 block text-xs tabular-nums text-[var(--text-secondary)]">
                {c.sub}
              </span>
            </dd>
          </div>
        ))}

        <div className="rounded-lg border border-[var(--border)] bg-[var(--surface-1)] p-4">
          <dt className="text-xs text-[var(--text-secondary)]">Long-run change</dt>
          <dd className="mt-1">
            {change === null ? (
              <>
                <span className="block text-lg font-semibold text-[var(--text-primary)]">
                  Not available
                </span>
                <span className="mt-0.5 block text-xs text-[var(--text-secondary)]">
                  {metric.cadence === "snapshot"
                    ? "Two snapshots four years apart are not a trend."
                    : "Fewer than two complete years."}
                </span>
              </>
            ) : (
              <>
                <span className="flex items-baseline gap-2">
                  <span
                    aria-hidden="true"
                    style={{ color: trendColour(change.improved) }}
                    className="text-lg"
                  >
                    {change.delta > 0 ? "↑" : "↓"}
                  </span>
                  <span className="text-lg font-semibold tabular-nums text-[var(--text-primary)]">
                    {formatDelta(change.delta, metric.scale)}
                  </span>
                  {/* The word, not just the arrow and the colour. */}
                  <span
                    className="text-xs font-medium"
                    style={{ color: trendColour(change.improved) }}
                  >
                    {change.improved === null
                      ? "no direction"
                      : change.improved
                        ? "improving"
                        : "worsening"}
                  </span>
                </span>
                <span className="mt-0.5 block text-xs tabular-nums text-[var(--text-secondary)]">
                  {describeYear(change.fromYear, metric)} → {describeYear(change.toYear, metric)},
                  borough mean
                  {metric.partial_years.length > 0 && ", skipping partial years"}
                </span>
              </>
            )}
          </dd>
        </div>
      </dl>
    </section>
  );
}

function trendColour(improved: boolean | null): string {
  if (improved === null) return "var(--text-secondary)";
  return improved ? "var(--delta-good)" : "var(--status-critical)";
}
