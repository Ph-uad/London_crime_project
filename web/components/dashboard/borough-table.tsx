"use client";

import { formatValue } from "@/lib/format";
import { ordinal, rankOf } from "@/lib/stats";
import type { BoroughRef, MetricCoverage } from "@/lib/types";

/**
 * The borough table (plan issues 3.2, 3.5, 3.8).
 *
 * This is not a decorative extra. A choropleth is an image, and an image cannot
 * be read by a screen reader, scrubbed by a keyboard, or compared precisely by
 * eye — three quantile classes apart still looks like "darker". The table is the
 * accessible equivalent of the map and the exact-value view for everyone: same
 * values, same selection state, same no-data cases, in one tab stop per row.
 *
 * It is also what makes the no-data state legible without colour, which is issue
 * 3.8's criterion: "no data" is a word here, not a shade of grey.
 */
export function BoroughTable({
  boroughs,
  values,
  metric,
  year,
  excluded,
  selected,
  hovered,
  onSelect,
  onHover,
  onToggleExclude,
}: {
  boroughs: readonly BoroughRef[];
  values: readonly (number | null)[];
  metric: MetricCoverage;
  year: number;
  excluded: ReadonlySet<string>;
  selected: number | null;
  hovered: number | null;
  onSelect: (index: number | null) => void;
  onHover: (index: number | null) => void;
  onToggleExclude: (gss: string) => void;
}) {
  // Worst first, so the top of the table is the top of the story whichever way
  // the metric runs. Boroughs with no value sort last rather than as zero.
  const order = boroughs
    .map((_, i) => i)
    .sort((a, b) => {
      const va = values[a];
      const vb = values[b];
      if (va === null && vb === null) return boroughs[a].name.localeCompare(boroughs[b].name);
      if (va === null) return 1;
      if (vb === null) return -1;
      return metric.direction === "higher_is_better" ? va - vb : vb - va;
    });

  const rankLabel =
    metric.direction === "neutral"
      ? "Rank (1 = highest)"
      : metric.direction === "higher_is_worse"
        ? "Rank (1 = highest, the worse end)"
        : "Rank (1 = lowest, the worse end)";

  return (
    <div className="mt-4">
      <div className="max-h-96 overflow-auto rounded-md border border-[var(--border)]">
        <table className="w-full border-collapse text-left text-sm">
          <caption className="sr-only">
            {metric.label} by borough, {year}. {rankLabel}. Selecting a row opens that
            borough&apos;s detail panel.
          </caption>
          <thead className="sticky top-0 bg-[var(--surface-1)]">
            <tr className="border-b border-[var(--border)]">
              <th scope="col" className="px-3 py-2 text-xs font-semibold text-[var(--text-secondary)]">
                <abbr title={rankLabel} className="no-underline">
                  #
                </abbr>
              </th>
              <th scope="col" className="px-3 py-2 text-xs font-semibold text-[var(--text-secondary)]">
                Borough
              </th>
              {/* The unit belongs in the header, once. Repeating "crimes per
                  1,000" on all 33 rows wraps every cell to four lines at
                  375 px and tells the reader nothing they did not know after
                  the first row. */}
              <th
                scope="col"
                className="px-3 py-2 text-right text-xs font-semibold text-[var(--text-secondary)]"
              >
                {metric.label}
                {metric.scale === "currency" || metric.scale === "proportion" ? null : (
                  <span className="block font-normal">{metric.unit}</span>
                )}
              </th>
              <th scope="col" className="px-3 py-2 text-right text-xs font-semibold text-[var(--text-secondary)]">
                In scale
              </th>
            </tr>
          </thead>
          <tbody>
            {order.map((i) => {
              const value = values[i];
              const isExcluded = excluded.has(boroughs[i].gss);
              const rank = isExcluded ? null : rankOf(values, i, metric.direction);
              const isActive = selected === i || hovered === i;

              return (
                <tr
                  key={boroughs[i].gss}
                  onPointerEnter={() => onHover(i)}
                  onPointerLeave={() => onHover(null)}
                  className={`border-b border-[var(--border)] last:border-0 ${
                    isActive ? "bg-[var(--gridline)]" : ""
                  }`}
                >
                  <td className="px-3 py-1.5 text-xs tabular-nums text-[var(--text-secondary)]">
                    {rank ? rank.rank : "—"}
                  </td>
                  <th scope="row" className="p-0 font-normal">
                    <button
                      type="button"
                      onClick={() => onSelect(selected === i ? null : i)}
                      onFocus={() => onHover(i)}
                      onBlur={() => onHover(null)}
                      aria-pressed={selected === i}
                      className="flex min-h-11 w-full items-center px-3 text-left text-[var(--text-primary)] underline-offset-2 hover:underline"
                    >
                      {boroughs[i].name}
                      {isExcluded && (
                        <span className="ml-2 rounded-sm border border-[var(--border)] px-1 text-[10px] uppercase tracking-wide text-[var(--text-secondary)]">
                          excluded
                        </span>
                      )}
                    </button>
                  </th>
                  <td className="px-3 py-1.5 text-right tabular-nums text-[var(--text-primary)]">
                    {value === null ? (
                      <span className="text-[var(--text-secondary)]">no data</span>
                    ) : (
                      formatValue(value, metric.scale)
                    )}
                    {rank && rank.of !== boroughs.length && (
                      <span className="ml-1 block text-xs text-[var(--text-secondary)]">
                        {ordinal(rank.rank)} of {rank.of}
                      </span>
                    )}
                  </td>
                  <td className="px-1 py-1.5 text-right">
                    <label className="inline-flex min-h-11 min-w-11 cursor-pointer items-center justify-center">
                      <input
                        type="checkbox"
                        checked={!isExcluded}
                        onChange={() => onToggleExclude(boroughs[i].gss)}
                        className="h-4 w-4 accent-[var(--series-1)]"
                      />
                      <span className="sr-only">
                        Include {boroughs[i].name} in the colour scale and charts
                      </span>
                    </label>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="mt-2 text-xs text-[var(--text-secondary)]">
        {rankLabel}. Ranks use the {metric.boroughs_covered} boroughs this metric covers, not
        always {boroughs.length}. Unticking a borough drops it from the colour scale, the
        correlation and the summary figures — it stays on the map, faded.
      </p>
    </div>
  );
}
