"use client";

import { useEffect, useRef, useState } from "react";

import { describeYear, formatCompact, formatWithUnit, YEAR_RULE_GLOSS } from "@/lib/format";
import { niceTicks } from "@/lib/scales";
import { groupByFamily, nearestYear, rowFor, type SeriesIndex } from "@/lib/series";
import { describeStrength, fitLine, type Pair } from "@/lib/stats";
import type { BoroughRef, MetricCoverage } from "@/lib/types";

/**
 * Crime against a social determinant (plan issue 3.6).
 *
 * This is the chart the project exists to produce, and the one most able to
 * mislead, so most of the code below is about what it refuses to do:
 *
 *   It never interpolates. Where the two series do not both publish the
 *   selected year, each is taken at its own nearest published year and BOTH
 *   years are printed on the chart. A pairing a reader can see is a pairing a
 *   reader can reject.
 *
 *   It names the year rules when they differ. Crime is a calendar year; income
 *   is a financial year labelled by its start; life expectancy is a three-year
 *   rolling period labelled by its end. "2019 × 2019" hides the fact that one of
 *   those covers 2017–2019 and the other April 2019 to March 2020.
 *
 *   It counts what it dropped. A borough missing from either series is out of
 *   the fit, and the number is on the chart rather than absorbed into a quieter
 *   n. City of London alone accounts for it on well-being and life expectancy.
 *
 *   It says "associates with", never "drives", "explains" or "causes". These are
 *   33 aggregated areas; an association between area averages is not a statement
 *   about people, and the footnote says so every time.
 */

const HEIGHT_RATIO = 0.62;
const MARGIN = { top: 12, right: 14, bottom: 52, left: 62 };
const DEFAULT_WIDTH = 640;

export function Scatterplot({
  boroughs,
  metrics,
  series,
  outcomeId,
  compareId,
  year,
  excluded,
  hovered,
  selected,
  onHover,
  onSelect,
  onCompare,
}: {
  boroughs: readonly BoroughRef[];
  metrics: Record<string, MetricCoverage>;
  series: SeriesIndex;
  outcomeId: string;
  compareId: string;
  year: number;
  excluded: ReadonlySet<string>;
  hovered: number | null;
  selected: number | null;
  onHover: (index: number | null) => void;
  onSelect: (index: number | null) => void;
  onCompare: (id: string) => void;
}) {
  const [width, setWidth] = useState(DEFAULT_WIDTH);
  const boxRef = useRef<HTMLDivElement | null>(null);

  // Measured rather than guessed: the SVG is drawn in real pixels, so a 12px
  // axis label is 12px at 375 px and at 1280 px. A fixed viewBox scaled by CSS
  // shrinks the type along with the chart, which is how a scatter becomes
  // unreadable on a phone while looking fine on the machine it was built on.
  useEffect(() => {
    const el = boxRef.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver(([entry]) => {
      const w = entry.contentRect.width;
      if (w > 0) setWidth(w);
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  const outcome = metrics[outcomeId];
  const compare = metrics[compareId];

  const yYear = outcome.years.includes(year) ? year : nearestYear(outcome, year);
  const xYear = compare.years.includes(year) ? year : nearestYear(compare, year);

  const yRow = yYear === null ? [] : rowFor(series, outcomeId, yYear, boroughs.length);
  const xRow = xYear === null ? [] : rowFor(series, compareId, xYear, boroughs.length);

  const pairs: Pair[] = [];
  let droppedMissing = 0;
  let droppedExcluded = 0;
  for (let i = 0; i < boroughs.length; i++) {
    if (excluded.has(boroughs[i].gss)) {
      droppedExcluded++;
      continue;
    }
    const x = xRow[i];
    const y = yRow[i];
    if (x === null || x === undefined || y === null || y === undefined) {
      droppedMissing++;
      continue;
    }
    pairs.push({ index: i, x, y });
  }

  const fit = fitLine(pairs);
  const height = Math.max(260, Math.round(width * HEIGHT_RATIO));
  const plotW = Math.max(10, width - MARGIN.left - MARGIN.right);
  const plotH = Math.max(10, height - MARGIN.top - MARGIN.bottom);

  const xs = pairs.map((p) => p.x);
  const ys = pairs.map((p) => p.y);
  const xDomain = padDomain(xs);
  const yDomain = padDomain(ys);

  const sx = (v: number) =>
    MARGIN.left + ((v - xDomain[0]) / (xDomain[1] - xDomain[0] || 1)) * plotW;
  const sy = (v: number) =>
    MARGIN.top + plotH - ((v - yDomain[0]) / (yDomain[1] - yDomain[0] || 1)) * plotH;

  const groups = groupByFamily(metrics);
  const sameYear = xYear === yYear;
  const rulesDiffer = compare.year_rule !== outcome.year_rule;

  const summary =
    fit === null
      ? `Not enough boroughs with both values to fit a line: ${pairs.length} of ${boroughs.length}.`
      : `${describeStrength(fit.r)} ${fit.r < 0 ? "negative" : "positive"} association, Pearson r = ${fit.r.toFixed(2)}, across ${fit.n} boroughs.`;

  return (
    <div>
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div className="min-w-0">
          <h2 className="text-sm font-semibold text-[var(--text-primary)]">
            {outcome.label} against {compare.label}
          </h2>
          {/* The pairing, stated. Not a footnote — the reader needs it to know
              what they are looking at. */}
          <p className="mt-0.5 text-xs text-[var(--text-secondary)]">
            {yYear === null || xYear === null ? (
              "One of these series has no published years."
            ) : (
              <>
                <span className="tabular-nums">{describeYear(yYear, outcome)}</span> ×{" "}
                <span className="tabular-nums">{describeYear(xYear, compare)}</span>
                {!sameYear && " — the nearest published year on each side, not interpolated"}
                {rulesDiffer && (
                  <>
                    {" "}
                    · {outcome.label} is a {YEAR_RULE_GLOSS[outcome.year_rule]}, {compare.label} a{" "}
                    {YEAR_RULE_GLOSS[compare.year_rule]}, so these are not the same twelve months
                  </>
                )}
              </>
            )}
          </p>
        </div>

        <div className="shrink-0">
          <label
            htmlFor="compare-select"
            className="block text-xs font-semibold text-[var(--text-secondary)]"
          >
            Compare against
          </label>
          <select
            id="compare-select"
            value={compareId}
            onChange={(e) => onCompare(e.target.value)}
            className="mt-1 min-h-11 rounded-md border border-[var(--border)] bg-[var(--surface-1)] px-3 text-sm text-[var(--text-primary)]"
          >
            {groups.map((g) => (
              <optgroup key={g.family} label={g.family}>
                {g.ids
                  .filter((id) => id !== outcomeId)
                  .map((id) => (
                    <option key={id} value={id}>
                      {metrics[id].label}
                    </option>
                  ))}
              </optgroup>
            ))}
          </select>
        </div>
      </div>

      <div ref={boxRef} className="mt-3 w-full">
        <svg
          role="img"
          aria-label={`Scatterplot of ${outcome.label} against ${compare.label}, one point per borough. ${summary} Exact values for every borough are in the borough table and the borough detail panel.`}
          width={width}
          height={height}
          viewBox={`0 0 ${width} ${height}`}
          className="w-full"
        >
          <defs>
            {/* The fitted line runs to the edges of the padded domain, which
                can be outside the value range on the other axis — a steep fit
                leaves the plot and draws over the axis labels. Clip it to the
                plotting rectangle so the line never implies a value the chart
                is not showing. */}
            <clipPath id="scatter-plot-area">
              <rect x={MARGIN.left} y={MARGIN.top} width={plotW} height={plotH} />
            </clipPath>
          </defs>

          {/* Gridlines and tick labels are chart chrome, which is what
              --text-muted and --gridline are for. Prose never uses them: at
              3.50:1 --text-muted is below the AA floor for text. */}
          {niceTicks(yDomain[0], yDomain[1], 5).map((t) => (
            <g key={`y${t}`}>
              <line
                x1={MARGIN.left}
                x2={MARGIN.left + plotW}
                y1={sy(t)}
                y2={sy(t)}
                style={{ stroke: "var(--gridline)", strokeWidth: 1 }}
              />
              <text
                x={MARGIN.left - 8}
                y={sy(t)}
                textAnchor="end"
                dominantBaseline="middle"
                style={{ fill: "var(--text-muted)", fontSize: 11 }}
              >
                {formatCompact(t, outcome.scale)}
              </text>
            </g>
          ))}

          {niceTicks(xDomain[0], xDomain[1], width < 480 ? 3 : 5).map((t) => (
            <g key={`x${t}`}>
              <line
                y1={MARGIN.top}
                y2={MARGIN.top + plotH}
                x1={sx(t)}
                x2={sx(t)}
                style={{ stroke: "var(--gridline)", strokeWidth: 1 }}
              />
              <text
                x={sx(t)}
                y={MARGIN.top + plotH + 16}
                textAnchor="middle"
                style={{ fill: "var(--text-muted)", fontSize: 11 }}
              >
                {formatCompact(t, compare.scale)}
              </text>
            </g>
          ))}

          <line
            x1={MARGIN.left}
            x2={MARGIN.left + plotW}
            y1={MARGIN.top + plotH}
            y2={MARGIN.top + plotH}
            style={{ stroke: "var(--baseline)", strokeWidth: 1 }}
          />
          <line
            x1={MARGIN.left}
            x2={MARGIN.left}
            y1={MARGIN.top}
            y2={MARGIN.top + plotH}
            style={{ stroke: "var(--baseline)", strokeWidth: 1 }}
          />

          <text
            x={MARGIN.left + plotW / 2}
            y={height - 6}
            textAnchor="middle"
            style={{ fill: "var(--text-secondary)", fontSize: 12 }}
          >
            {compare.label}
            {compare.scale === "currency" || compare.scale === "proportion"
              ? ""
              : ` (${compare.unit})`}
          </text>
          <text
            transform={`translate(14 ${MARGIN.top + plotH / 2}) rotate(-90)`}
            textAnchor="middle"
            style={{ fill: "var(--text-secondary)", fontSize: 12 }}
          >
            {outcome.label}
          </text>

          {/* The fitted line is drawn only across the range that has data, so it
              does not imply a prediction beyond the observed boroughs. */}
          {fit && (
            <line
              clipPath="url(#scatter-plot-area)"
              x1={sx(xDomain[0])}
              y1={sy(fit.intercept + fit.slope * xDomain[0])}
              x2={sx(xDomain[1])}
              y2={sy(fit.intercept + fit.slope * xDomain[1])}
              style={{ stroke: "var(--series-2)", strokeWidth: 2, strokeDasharray: "6 4" }}
            />
          )}

          {pairs.map((p) => {
            const active = hovered === p.index || selected === p.index;
            return (
              <circle
                key={boroughs[p.index].gss}
                cx={sx(p.x)}
                cy={sy(p.y)}
                r={active ? 8 : 5}
                style={{
                  fill: "var(--series-1)",
                  stroke: active ? "var(--text-primary)" : "var(--surface-1)",
                  strokeWidth: active ? 2.5 : 1,
                  cursor: "pointer",
                }}
                onPointerEnter={() => onHover(p.index)}
                onPointerLeave={() => onHover(null)}
                onClick={() => onSelect(selected === p.index ? null : p.index)}
              />
            );
          })}

          {/* Only the active point is labelled. Thirty-three labels on a phone
              is a solid block of overlapping text. */}
          {pairs
            .filter((p) => hovered === p.index || selected === p.index)
            .map((p) => (
              <text
                key={`label-${boroughs[p.index].gss}`}
                x={Math.min(sx(p.x) + 12, MARGIN.left + plotW)}
                y={sy(p.y) - 12}
                textAnchor={sx(p.x) > MARGIN.left + plotW * 0.7 ? "end" : "start"}
                style={{
                  fill: "var(--text-primary)",
                  fontSize: 12,
                  fontWeight: 600,
                  paintOrder: "stroke",
                  stroke: "var(--surface-1)",
                  strokeWidth: 4,
                  strokeLinejoin: "round",
                }}
              >
                {boroughs[p.index].name}
              </text>
            ))}
        </svg>
      </div>

      <div className="mt-2 space-y-1 text-xs text-[var(--text-secondary)]">
        <p>
          {fit ? (
            <>
              <span className="font-semibold text-[var(--text-primary)]">
                Pearson r = <span className="tabular-nums">{fit.r.toFixed(2)}</span>
              </span>{" "}
              — a {describeStrength(fit.r)} {fit.r < 0 ? "negative" : "positive"} association across{" "}
              <span className="tabular-nums">{fit.n}</span> boroughs.
            </>
          ) : (
            <span className="font-semibold text-[var(--text-primary)]">
              No line is fitted: {pairs.length < 3
                ? `only ${pairs.length} boroughs have both values.`
                : `${compare.label} has no variation between boroughs at this precision, so a slope would be meaningless.`}
            </span>
          )}
          {droppedMissing > 0 && (
            <>
              {" "}
              <span className="tabular-nums">{droppedMissing}</span>{" "}
              {droppedMissing === 1 ? "borough is" : "boroughs are"} not plotted because one of the
              two series does not cover {droppedMissing === 1 ? "it" : "them"}.
            </>
          )}
          {droppedExcluded > 0 && (
            <>
              {" "}
              <span className="tabular-nums">{droppedExcluded}</span>{" "}
              {droppedExcluded === 1 ? "borough was" : "boroughs were"} excluded by you.
            </>
          )}
        </p>
        <p>
          <strong className="text-[var(--text-primary)]">Association, not cause.</strong> These are
          33 aggregated borough averages. A relationship between area averages does not hold for
          the people inside those areas, and neither variable is shown to act on the other — both
          may follow from something not measured here.
        </p>
      </div>
    </div>
  );
}

/** A domain with 5% of slack either side, so points do not sit on the axes. */
function padDomain(values: readonly number[]): [number, number] {
  if (!values.length) return [0, 1];
  const lo = Math.min(...values);
  const hi = Math.max(...values);
  if (lo === hi) return [lo - 1, hi + 1];
  const pad = (hi - lo) * 0.05;
  return [lo - pad, hi + pad];
}

/** Re-exported for the KPI panel's tooltip text. */
export function describePoint(
  borough: BoroughRef,
  value: number,
  metric: MetricCoverage,
): string {
  return `${borough.name}: ${formatWithUnit(value, metric)}`;
}
