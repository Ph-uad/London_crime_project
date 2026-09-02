/**
 * The coverage matrix, and only the coverage matrix.
 *
 * This module exists to keep `boroughs.json` out of the browser. The split is
 * not cosmetic — it was a measured 516 KB regression:
 *
 *   `components/site-header.tsx` is a client component. It imports `lib/site.ts`
 *   for the nav labels, `lib/site.ts` imported `lib/data.ts` for the borough
 *   count, and `lib/data.ts` imports the full observation export. Turbopack
 *   cannot drop the JSON — the module derives `observations` and `allYears`
 *   from it at module scope — so every visitor downloaded all 6,001
 *   observations in order to render the word "Dashboard" in the header.
 *   Confirmed by grepping the built chunk: 6,001 `{"borough_gss":…}` objects.
 *
 * So: anything a client component may reach imports from HERE. `coverage.json`
 * is 9 KB and is the contract the UI genuinely needs — labels, units, direction,
 * cadence, years and per-metric borough coverage. The bulk observation export
 * stays behind the `server-only` marker in `lib/data.ts`, and the server passes
 * down the compact index built by `lib/series.ts`.
 */
import coverageJson from "@data/coverage.json";

import type { BoroughRef, CoverageMatrix, MetricCoverage } from "./types";

export const coverage = coverageJson as unknown as CoverageMatrix;

export const boroughs: readonly BoroughRef[] = coverage.boroughs;
export const analysisWindow = coverage.window;

/** Metric ids the API will serve, sorted for stable error messages. */
export const metricIds: readonly string[] = Object.keys(coverage.metrics).sort();

/** GSS codes of all 33 boroughs — the union across metrics, not any one metric's coverage. */
export const boroughCodes: ReadonlySet<string> = new Set(boroughs.map((b) => b.gss));

export function metricCoverage(id: string): MetricCoverage | undefined {
  return coverage.metrics[id];
}
