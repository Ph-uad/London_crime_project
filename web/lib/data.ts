import "server-only";

/**
 * The full borough observation export : server-side only.
 *
 * `boroughs.json` and `coverage.json` are imported through the `@data/*` alias
 * (see tsconfig paths → `../data/processed/*`), so they are resolved and bundled
 * at build time. That is deliberate: the data only changes on a redeploy, which
 * is exactly the assumption behind the long `s-maxage` in lib/http.ts. No copy
 * step, no second source of truth, no chance of the API serving a stale copy the
 * pipeline has already replaced.
 *
 * WHY `server-only`. This module pulls in 516 KB of observations. Nothing in the
 * browser needs them: the dashboard runs on the compact index built by
 * `lib/series.ts` and passed down as a prop, and the API routes serve the rest
 * over HTTP. The marker turns "a client component reached this" from a silent
 * half-megabyte in the bundle : which is what it was, measured in the built
 * chunk : into a build failure naming the cause.
 *
 * Client-reachable code imports `lib/coverage.ts` instead. The coverage matrix
 * is re-exported here so server callers still have one import site.
 */
import boroughsJson from "@data/boroughs.json";

import { coverage } from "./coverage";
import type { BoroughRef, BoroughsExport, MetricCoverage, Observation } from "./types";

export {
  analysisWindow,
  boroughCodes,
  boroughs,
  coverage,
  metricCoverage,
  metricIds,
} from "./coverage";

export const boroughsExport = boroughsJson as unknown as BoroughsExport;

export const observations: readonly Observation[] = boroughsExport.observations;

/** Every year present anywhere in the export. */
export const allYears: readonly number[] = Array.from(
  new Set(observations.map((o) => o.year)),
).sort((a, b) => a - b);

/**
 * The observations matching a filter. `null` for a field means "no filter on it".
 * Filters are validated by the caller; this assumes the ids are already known good.
 */
export function selectObservations(filters: {
  metric: string[] | null;
  year: number[] | null;
  borough: string[] | null;
}): Observation[] {
  const metrics = filters.metric ? new Set(filters.metric) : null;
  const years = filters.year ? new Set(filters.year) : null;
  const codes = filters.borough ? new Set(filters.borough) : null;

  return observations.filter(
    (o) =>
      (metrics === null || metrics.has(o.metric)) &&
      (years === null || years.has(o.year)) &&
      (codes === null || codes.has(o.borough_gss)),
  );
}

/** Coverage entries for exactly the metrics present in a result set. */
export function coverageFor(rows: readonly Observation[]): Record<string, MetricCoverage> {
  const out: Record<string, MetricCoverage> = {};
  for (const id of new Set(rows.map((r) => r.metric))) {
    const c = coverage.metrics[id];
    if (c) out[id] = c;
  }
  return out;
}

/** Re-exported so callers do not need a second import for the borough list type. */
export type { BoroughRef };
