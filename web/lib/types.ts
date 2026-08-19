/**
 * Shared response types for the data API (plan issues 2.1 and 2.2).
 *
 * These mirror `data/processed/coverage.json` and `boroughs.json` IN FULL, not a
 * convenient subset. The pipeline puts five things in the coverage matrix that the
 * frontend cannot safely infer, and narrowing the types here would quietly remove
 * them from Epic 3's reach:
 *
 *   direction   falling crime is good, falling life expectancy is not
 *   scale       IMD domains span proportion, score and standardised
 *   cadence     IMD is two snapshots, not an annual series
 *   year_rule   a calendar year and a rolling-period end year are not the same year
 *   boroughsMissing  City of London is absent from two metrics — 32, not 33
 *
 * Field names follow the JSON exactly (snake_case) so a type error appears if the
 * pipeline's contract changes, rather than a silent `undefined` at runtime.
 */

/** Which way is "good". Never assume — read it per metric. */
export type Direction = "higher_is_better" | "higher_is_worse" | "neutral";

/** `annual` drives a continuous slider; `snapshot` drives discrete points. */
export type Cadence = "annual" | "snapshot";

/** Units family. Metrics on different scales must not share a colour ramp. */
export type ScaleType =
  | "rate"
  | "count"
  | "currency"
  | "proportion"
  | "score"
  | "standardised"
  | "rating"
  | "years";

/** How the metric's `year` was derived from its source period. */
export type YearRule = "calendar" | "financial_start" | "rolling_end" | "snapshot";

export interface BoroughRef {
  gss: string;
  name: string;
}

export interface AnalysisWindow {
  analysis_start: number;
  analysis_end: number;
  trend_end: number;
  note: string;
}

export interface MetricCoverage {
  label: string;
  cadence: Cadence;
  direction: Direction;
  scale: ScaleType;
  unit: string;
  year_rule: YearRule;
  /** Years with at least one observation. Always an array, even at length 1. */
  years: number[];
  /** Years that are not twelve months of data. Excluded from year-on-year comparison. */
  partial_years: number[];
  boroughs_covered: number;
  /** Boroughs this metric does not cover, with names. Empty for most metrics. */
  boroughs_missing: BoroughRef[];
  observations: number;
  source: string;
}

/** `data/processed/coverage.json` as published by pipeline/20_unify_metrics.R. */
export interface CoverageMatrix {
  generated_utc: string;
  window: AnalysisWindow;
  boroughs: BoroughRef[];
  metrics: Record<string, MetricCoverage>;
}

export interface Observation {
  borough_gss: string;
  year: number;
  metric: string;
  value: number;
}

/** `data/processed/boroughs.json` as published by pipeline/20_unify_metrics.R. */
export interface BoroughsExport {
  generated_utc: string;
  window: AnalysisWindow;
  schema: string[];
  note: string;
  boroughs: BoroughRef[];
  observations: Observation[];
}

/** Echo of the filters actually applied, so a client can tell what it asked for. */
export interface AppliedFilters {
  metric: string[] | null;
  year: number[] | null;
  borough: string[] | null;
}

/**
 * Response envelope for `GET /api/metrics`.
 *
 * `coverage` carries the matrix entry for every metric in `observations`. That is
 * what satisfies issue 2.1's "responses include partial flags so clients cannot
 * mistake 4-month 2026 for a year" — the flags travel with the data rather than
 * requiring a second call the client might skip.
 */
export interface MetricsResponse {
  generated_utc: string;
  window: AnalysisWindow;
  filters: AppliedFilters;
  count: number;
  boroughs: BoroughRef[];
  coverage: Record<string, MetricCoverage>;
  observations: Observation[];
}

/** Every 4xx body has this shape. `valid` is populated where a list is short enough to help. */
export interface ApiError {
  error: string;
  detail: string;
  parameter?: string;
  valid?: string[];
}

export type GeoJsonPolygon = {
  type: "Polygon" | "MultiPolygon";
  coordinates: unknown[];
};

export interface BoroughFeature {
  type: "Feature";
  properties: { borough_gss: string; borough_name: string };
  geometry: GeoJsonPolygon;
}

/** `data/processed/london.geojson` — 33 features, EPSG:4326, RFC 7946. */
export interface BoroughFeatureCollection {
  type: "FeatureCollection";
  features: BoroughFeature[];
  bbox?: number[];
}
