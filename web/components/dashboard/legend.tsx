"use client";

import { formatValue } from "@/lib/format";
import { legendRows, NO_DATA_PATTERN_ID, type ColourScale } from "@/lib/scales";
import type { MetricCoverage } from "@/lib/types";

/**
 * The choropleth legend (plan issue 3.2).
 *
 * It prints the real break values rather than "low → high", because the classes
 * are quantiles: seven evenly sized swatches would otherwise imply seven evenly
 * spaced value ranges, which is exactly what quantile classing is not. The
 * units come from the metric's `unit`, and the note states which end of the ramp
 * is the worse one : a `higher_is_worse` metric and a `higher_is_better` one use
 * the same colours for opposite values, so leaving that to be inferred is how a
 * reader concludes that rich boroughs have the most crime.
 */
export function Legend({
  scale,
  metric,
  excludedCount,
  noDataCount,
}: {
  scale: ColourScale;
  metric: MetricCoverage;
  excludedCount: number;
  noDataCount: number;
}) {
  const rows = legendRows(scale, metric.scale);

  return (
    <div className="mt-3">
      <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
        <h3 className="text-sm font-semibold text-[var(--text-primary)]">{metric.label}</h3>
        <span className="text-xs text-[var(--text-secondary)]">
          {metric.scale === "currency" || metric.scale === "proportion" ? null : metric.unit}
        </span>
      </div>

      {rows.length > 0 && (
        <ul className="mt-2 flex flex-wrap gap-x-4 gap-y-1.5" role="list">
          {rows.map((row, i) => (
            <li key={i} className="flex items-center gap-1.5 text-xs text-[var(--text-secondary)]">
              <Swatch fill={row.fill} />
              <span className="tabular-nums">{row.label}</span>
            </li>
          ))}
          {noDataCount > 0 && (
            <li className="flex items-center gap-1.5 text-xs text-[var(--text-secondary)]">
              <HatchSwatch />
              <span>
                No data ({noDataCount} {noDataCount === 1 ? "borough" : "boroughs"})
              </span>
            </li>
          )}
        </ul>
      )}

      <p className="mt-2 max-w-prose text-xs text-[var(--text-secondary)]">
        {scale.note}
        {scale.domain && !scale.degenerate && (
          <>
            {" "}
            Range across the {scale.n} boroughs shown:{" "}
            <span className="tabular-nums">
              {formatValue(scale.domain[0], metric.scale)}–
              {formatValue(scale.domain[1], metric.scale)}
            </span>
            .
          </>
        )}
        {excludedCount > 0 && (
          <>
            {" "}
            {excludedCount} {excludedCount === 1 ? "borough is" : "boroughs are"} excluded from the
            scale and drawn faded.
          </>
        )}
      </p>
    </div>
  );
}

function Swatch({ fill }: { fill: string }) {
  return (
    <span
      aria-hidden="true"
      className="inline-block h-3.5 w-6 shrink-0 rounded-sm border border-[var(--border)]"
      style={{ background: fill }}
    />
  );
}

/** The no-data hatch, redrawn as a tiny SVG so the legend matches the map. */
function HatchSwatch() {
  return (
    <svg aria-hidden="true" width="24" height="14" className="shrink-0 rounded-sm">
      <defs>
        <pattern
          id={`${NO_DATA_PATTERN_ID}-legend`}
          width="6"
          height="6"
          patternUnits="userSpaceOnUse"
          patternTransform="rotate(45)"
        >
          <rect width="6" height="6" style={{ fill: "var(--no-data)" }} />
          <line
            x1="0"
            y1="0"
            x2="0"
            y2="6"
            style={{ stroke: "var(--no-data-ink)", strokeWidth: 1.6, opacity: 0.55 }}
          />
        </pattern>
      </defs>
      <rect
        width="24"
        height="14"
        rx="2"
        style={{
          fill: `url(#${NO_DATA_PATTERN_ID}-legend)`,
          stroke: "var(--border)",
          strokeWidth: 1,
        }}
      />
    </svg>
  );
}
