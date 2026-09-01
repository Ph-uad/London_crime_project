"use client";

import { YEAR_RULE_GLOSS } from "@/lib/format";
import { groupByFamily } from "@/lib/series";
import type { BoroughRef, MetricCoverage } from "@/lib/types";

/**
 * Metric switcher and exclusions (plan issue 3.3).
 *
 * The list is built from the coverage matrix — ids, labels and grouping all come
 * from the data, so a metric added to the pipeline appears here without a code
 * change and a metric removed cannot linger as a dead option. Nothing about the
 * 19 metrics is typed into this file.
 *
 * A native `<select>` with `<optgroup>` rather than a custom listbox: it is
 * keyboard-accessible and screen-reader-correct without any ARIA of ours, and on
 * a phone it opens the platform picker, which is a better control at 375 px than
 * anything reimplemented in a div.
 *
 * The per-borough exclusion checkboxes live in the borough table rather than
 * here — 33 checkboxes in a sidebar is a wall, and the table already lists every
 * borough with the value the exclusion would affect. This panel carries the one
 * exclusion the data actually argues for.
 */
export function MetricControls({
  metrics,
  metric,
  metricId,
  boroughs,
  excluded,
  onMetric,
  onToggleExclude,
  onClearExclusions,
}: {
  metrics: Record<string, MetricCoverage>;
  metric: MetricCoverage;
  metricId: string;
  boroughs: readonly BoroughRef[];
  excluded: ReadonlySet<string>;
  onMetric: (id: string) => void;
  onToggleExclude: (gss: string) => void;
  onClearExclusions: () => void;
}) {
  const groups = groupByFamily(metrics);
  const outlier = boroughs.find((b) => b.gss === "E09000001");

  return (
    <div className="space-y-4">
      <div>
        <label
          htmlFor="metric-select"
          className="block text-xs font-semibold text-[var(--text-secondary)]"
        >
          Map this metric
        </label>
        <select
          id="metric-select"
          value={metricId}
          onChange={(e) => onMetric(e.target.value)}
          className="mt-1 min-h-11 w-full rounded-md border border-[var(--border)] bg-[var(--surface-1)] px-3 text-sm text-[var(--text-primary)]"
        >
          {groups.map((g) => (
            <optgroup key={g.family} label={g.family}>
              {g.ids.map((id) => (
                <option key={id} value={id}>
                  {metrics[id].label}
                </option>
              ))}
            </optgroup>
          ))}
        </select>

        {/* Everything a reader needs to interpret the selection, straight from
            the contract: what it is measured in, how its year is defined, how
            many boroughs it covers, and where its series stops. */}
        <dl className="mt-2 space-y-0.5 text-xs text-[var(--text-secondary)]">
          <div className="flex gap-1">
            <dt className="sr-only">Source</dt>
            <dd>{metric.source}</dd>
          </div>
          <div className="flex gap-1">
            <dt className="sr-only">Coverage</dt>
            <dd>
              <span className="tabular-nums">
                {Math.min(...metric.years)}–{Math.max(...metric.years)}
              </span>{" "}
              · {YEAR_RULE_GLOSS[metric.year_rule]} · {metric.boroughs_covered} of{" "}
              {boroughs.length} boroughs
            </dd>
          </div>
          {metric.boroughs_missing.length > 0 && (
            <div>
              <dt className="sr-only">Not covered</dt>
              <dd>
                No data for {metric.boroughs_missing.map((b) => b.name).join(", ")}.
              </dd>
            </div>
          )}
          {metric.cadence === "snapshot" && (
            <div>
              <dt className="sr-only">Cadence</dt>
              <dd>
                The {metric.years.join(" and ")} indices are separate exercises whose ranks are not
                comparable, so scores are used and no trend is drawn between them.
              </dd>
            </div>
          )}
        </dl>
      </div>

      <fieldset className="border-t border-[var(--border)] pt-3">
        <legend className="text-xs font-semibold text-[var(--text-secondary)]">
          Boroughs in the scale
        </legend>
        <p className="mt-1 text-xs text-[var(--text-secondary)]">
          Excluded boroughs are dropped from the colour classes, the correlation and the summary
          figures. Use the table below for the full list.
        </p>

        {outlier && (
          <label className="mt-2 flex min-h-11 items-start gap-2 py-2 text-sm text-[var(--text-primary)]">
            <input
              type="checkbox"
              checked={excluded.has(outlier.gss)}
              onChange={() => onToggleExclude(outlier.gss)}
              className="mt-0.5 h-4 w-4 shrink-0 accent-[var(--series-1)]"
            />
            <span>
              Exclude {outlier.name}
              <span className="block text-xs text-[var(--text-secondary)]">
                About 8,000 residents, so its per-capita rates are extreme — a crime rate around
                six times the London median.
              </span>
            </span>
          </label>
        )}

        {excluded.size > 0 && (
          <button
            type="button"
            onClick={onClearExclusions}
            className="mt-2 min-h-11 rounded-md border border-[var(--border)] px-3 text-sm text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
          >
            Clear {excluded.size} exclusion{excluded.size === 1 ? "" : "s"}
          </button>
        )}
      </fieldset>
    </div>
  );
}
