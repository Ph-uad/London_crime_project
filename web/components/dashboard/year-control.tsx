"use client";

import { describeYear } from "@/lib/format";
import type { MetricCoverage } from "@/lib/types";

/**
 * The year control (plan issue 3.4).
 *
 * Two controls, not one, chosen by the metric's `cadence`:
 *
 *   annual    a slider over the years the metric actually publishes. It is
 *             indexed by POSITION in that list, not by the year number, so a gap
 *             in the series cannot be scrubbed into. Dragging the handle through
 *             a year the source never collected would be interpolation by
 *             interface.
 *   snapshot  radio buttons. IMD is two snapshots four years apart; a slider
 *             across them would invite a reader to look for 2017 and read the
 *             absence as a dip.
 *
 * The range comes from the selected metric, never from the global window. Crime
 * runs to 2024 and well-being stops at 2022, so a shared slider would leave two
 * years of empty map with no explanation.
 *
 * Partial years are marked and, where the metric has any, called out when
 * selected: `crime_count` runs to 2026, but 2026 is four months of data and
 * comparing it with a full year is the single easiest mistake this dataset
 * invites.
 */
export function YearControl({
  metric,
  year,
  onYear,
}: {
  metric: MetricCoverage;
  year: number;
  onYear: (year: number) => void;
}) {
  const years = [...metric.years].sort((a, b) => a - b);
  const partial = new Set(metric.partial_years);
  const index = Math.max(0, years.indexOf(year));
  const isPartial = partial.has(year);

  return (
    <div className="border-t border-[var(--border)] pt-3">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <span id="year-control-label" className="text-xs font-semibold text-[var(--text-secondary)]">
          Year
        </span>
        {/* A plain span, not an <output>. `<output>` carries an implicit
            role="status", which makes it a live region that re-announces on
            every tick of the slider : on top of the slider's own aria-valuetext,
            which already says the same thing. Two announcements of one value is
            worse than one. */}
        <span className="text-lg font-semibold tabular-nums text-[var(--text-primary)]">
          {describeYear(year, metric)}
        </span>
      </div>

      {metric.cadence === "snapshot" ? (
        <fieldset className="mt-2">
          <legend className="sr-only">Snapshot year for {metric.label}</legend>
          <div className="flex flex-wrap gap-2">
            {years.map((y) => (
              <label
                key={y}
                className={`flex min-h-11 cursor-pointer items-center gap-2 rounded-md border px-3 text-sm ${
                  y === year
                    ? "border-[var(--series-1)] font-semibold text-[var(--text-primary)]"
                    : "border-[var(--border)] text-[var(--text-secondary)]"
                }`}
              >
                <input
                  type="radio"
                  name="snapshot-year"
                  value={y}
                  checked={y === year}
                  onChange={() => onYear(y)}
                  className="h-4 w-4 accent-[var(--series-1)]"
                />
                <span className="tabular-nums">{y}</span>
              </label>
            ))}
          </div>
        </fieldset>
      ) : (
        <>
          <input
            id="year-input"
            type="range"
            min={0}
            max={years.length - 1}
            step={1}
            value={index}
            onChange={(e) => onYear(years[Number(e.target.value)])}
            aria-labelledby="year-control-label"
            aria-valuetext={describeYear(years[index], metric)}
            // 44px of touch target: the native track is ~4px, so the height is
            // set on the input itself rather than left to the browser.
            className="mt-2 h-11 w-full cursor-pointer accent-[var(--series-1)]"
          />
          <div className="flex justify-between text-xs tabular-nums text-[var(--text-secondary)]">
            <span>{years[0]}</span>
            <span>{years[years.length - 1]}</span>
          </div>
        </>
      )}

      {partial.size > 0 && (
        <p className="mt-1 text-xs text-[var(--text-secondary)]">
          {[...partial].sort((a, b) => a - b).join(", ")} {partial.size === 1 ? "is" : "are"} not a
          full twelve months and {partial.size === 1 ? "is" : "are"} excluded from year-on-year
          comparisons.
        </p>
      )}

      {isPartial && (
        <p
          role="status"
          className="mt-2 rounded-md border border-[var(--status-warning)] px-2 py-1.5 text-xs text-[var(--text-primary)]"
        >
          <strong>{year} is a partial year.</strong> It holds fewer than twelve months of data, so
          its total is not comparable with any other year on this chart.
        </p>
      )}
    </div>
  );
}
