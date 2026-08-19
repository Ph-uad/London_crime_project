/**
 * The single place that knows where pipeline output lives.
 *
 * `boroughs.json` and `coverage.json` are imported through the `@data/*` alias
 * (see tsconfig paths → `../data/processed/*`), so they are resolved and bundled
 * at build time. That is deliberate: the data only changes on a redeploy, which
 * is exactly the assumption behind the long `s-maxage` in lib/http.ts. No copy
 * step, no second source of truth, no chance of the API serving a stale copy the
 * pipeline has already replaced.
 *
 * `london.geojson` cannot use the same route — TypeScript's `resolveJsonModule`
 * and the bundler only treat `.json` as a JSON module, and `.geojson` is not
 * that. It is read from disk instead, with `next.config.ts` tracing the file into
 * the deployment bundle. Renaming the pipeline output to `.json` would have been
 * simpler, but `london.geojson` is what issue 1.10's acceptance criteria name.
 */
import { readFileSync } from "node:fs";
import path from "node:path";

import boroughsJson from "@data/boroughs.json";
import coverageJson from "@data/coverage.json";

import type {
  BoroughFeatureCollection,
  BoroughRef,
  BoroughsExport,
  CoverageMatrix,
  MetricCoverage,
  Observation,
} from "./types";

export const boroughsExport = boroughsJson as unknown as BoroughsExport;
export const coverage = coverageJson as unknown as CoverageMatrix;

export const observations: readonly Observation[] = boroughsExport.observations;
export const boroughs: readonly BoroughRef[] = coverage.boroughs;
export const analysisWindow = coverage.window;

/** Metric ids the API will serve, sorted for stable error messages. */
export const metricIds: readonly string[] = Object.keys(coverage.metrics).sort();

/** GSS codes of all 33 boroughs — the union across metrics, not any one metric's coverage. */
export const boroughCodes: ReadonlySet<string> = new Set(boroughs.map((b) => b.gss));

/** Sorting Every year present anywhere in the export. */
export const allYears: readonly number[] = Array.from(
  new Set(observations.map((o) => o.year)),
).sort((a, b) => a - b);

export function metricCoverage(id: string): MetricCoverage | undefined {
  return coverage.metrics[id];
}

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

/**
 * The borough polygons. Read lazily and cached, so a build that never calls
 * /api/geo does not pay for it, and a missing file surfaces as a clear API error
 * rather than a crash at module load.
 */
let geoCache: BoroughFeatureCollection | null = null;

export const GEOJSON_PATH = path.join(
  process.cwd(),
  "..",
  "data",
  "processed",
  "london.geojson",
);

export function boroughGeoJson(): BoroughFeatureCollection {
  if (geoCache) return geoCache;
  const raw = readFileSync(GEOJSON_PATH, "utf8");
  geoCache = JSON.parse(raw) as BoroughFeatureCollection;
  return geoCache;
}
