/**
 * Dashboard state as query parameters (plan issue 3.3, "shareable views").
 *
 *   ?metric=crime_rate_per_1000     the mapped metric
 *   &year=2023                      the active year
 *   &compare=income_median          the scatter's x-axis metric
 *   &exclude=E09000001              boroughs dropped from scales and fits
 *   &borough=E09000007              the borough whose detail panel is open
 *
 * Two deliberate differences from the API's parameter handling in `lib/http.ts`:
 *
 *   Unknown parameters are ignored rather than rejected. The API rejects them
 *   because a silently dropped `?metrics=` returns the whole dataset and looks
 *   like it worked. A page URL is different : it collects `utm_source` and
 *   friends from anything that links to it, and refusing to render because of a
 *   tracking parameter would be a worse failure than ignoring one.
 *
 *   A bad VALUE falls back to the default instead of erroring, but the fallback
 *   is recorded in `rejected` and shown in the UI. A stale shared link should
 *   still open; it should just not pretend it opened what it was asked for.
 */
import type { CoverageMatrix, MetricCoverage } from "./types";

export interface DashboardState {
  metric: string;
  year: number;
  compare: string;
  exclude: string[];
  borough: string | null;
}

export interface ParsedState {
  state: DashboardState;
  /** Human-readable notes about values that could not be honoured. */
  rejected: string[];
}

export const PARAM_KEYS = ["metric", "year", "compare", "exclude", "borough"] as const;

/**
 * The metric the map opens on. Crime is the project's outcome variable, so the
 * dashboard opens on it and the reader chooses what to compare it against.
 */
export const DEFAULT_METRIC = "crime_rate_per_1000";
export const DEFAULT_COMPARE = "income_median";

/** Latest year of a metric that is not flagged partial. */
export function defaultYear(metric: MetricCoverage): number {
  const partial = new Set(metric.partial_years);
  const usable = metric.years.filter((y) => !partial.has(y));
  const years = usable.length ? usable : metric.years;
  return Math.max(...years);
}

/**
 * Move a year onto a metric's own range.
 *
 * Well-being stops at 2022 while crime runs to 2024, so switching metric with
 * 2024 selected has to land somewhere. It lands on the nearest year the metric
 * actually publishes, and the caller says so rather than showing an empty map.
 */
export function snapYear(metric: MetricCoverage, year: number): number {
  if (metric.years.includes(year)) return year;
  let best = metric.years[0];
  let gap = Math.abs(best - year);
  for (const y of metric.years) {
    const d = Math.abs(y - year);
    if (d < gap) {
      best = y;
      gap = d;
    }
  }
  return best;
}

function firstValue(params: URLSearchParams | Record<string, string | string[] | undefined>, key: string): string | null {
  if (params instanceof URLSearchParams) return params.get(key);
  const raw = params[key];
  if (raw === undefined) return null;
  return Array.isArray(raw) ? (raw[0] ?? null) : raw;
}

export function parseState(
  params: URLSearchParams | Record<string, string | string[] | undefined>,
  coverage: CoverageMatrix,
): ParsedState {
  const rejected: string[] = [];
  const metrics = coverage.metrics;
  const codes = new Set(coverage.boroughs.map((b) => b.gss));

  const pickMetric = (key: string, fallback: string): string => {
    const raw = firstValue(params, key);
    if (raw === null) return fallback;
    if (metrics[raw]) return raw;
    rejected.push(`'${raw}' is not a metric in this dataset; showing ${metrics[fallback].label} instead.`);
    return fallback;
  };

  const metric = pickMetric("metric", DEFAULT_METRIC);
  const compare = pickMetric("compare", DEFAULT_COMPARE);
  const cov = metrics[metric];

  let year = defaultYear(cov);
  const rawYear = firstValue(params, "year");
  if (rawYear !== null) {
    const parsed = Number.parseInt(rawYear, 10);
    if (!/^\d{4}$/.test(rawYear) || Number.isNaN(parsed)) {
      rejected.push(`'${rawYear}' is not a four-digit year; showing ${year}.`);
    } else if (!cov.years.includes(parsed)) {
      const snapped = snapYear(cov, parsed);
      rejected.push(
        `${cov.label} has no data for ${parsed} : its series runs ${Math.min(...cov.years)}–${Math.max(...cov.years)}. Showing ${snapped}.`,
      );
      year = snapped;
    } else {
      year = parsed;
    }
  }

  const exclude: string[] = [];
  const rawExclude = firstValue(params, "exclude");
  if (rawExclude !== null) {
    for (const code of rawExclude.split(",").map((c) => c.trim()).filter(Boolean)) {
      if (codes.has(code)) {
        if (!exclude.includes(code)) exclude.push(code);
      } else {
        rejected.push(`'${code}' is not a London borough GSS code; it was not excluded.`);
      }
    }
    // Excluding all 33 leaves nothing to scale against, which is a blank map
    // with no explanation. Refuse the whole list rather than render that.
    if (exclude.length >= coverage.boroughs.length) {
      rejected.push("Every borough was excluded, which leaves nothing to compare. The exclusions were cleared.");
      exclude.length = 0;
    }
  }

  let borough: string | null = null;
  const rawBorough = firstValue(params, "borough");
  if (rawBorough !== null) {
    if (codes.has(rawBorough)) borough = rawBorough;
    else rejected.push(`'${rawBorough}' is not a London borough GSS code; no borough was opened.`);
  }

  return { state: { metric, year, compare, exclude, borough }, rejected };
}

/**
 * State back to a query string, omitting anything at its default so a shared URL
 * carries only what the reader actually changed.
 */
export function toSearchString(state: DashboardState, coverage: CoverageMatrix): string {
  const params = new URLSearchParams();
  if (state.metric !== DEFAULT_METRIC) params.set("metric", state.metric);

  const cov = coverage.metrics[state.metric];
  if (cov && state.year !== defaultYear(cov)) params.set("year", String(state.year));

  if (state.compare !== DEFAULT_COMPARE) params.set("compare", state.compare);
  if (state.exclude.length) params.set("exclude", state.exclude.join(","));
  if (state.borough) params.set("borough", state.borough);

  const query = params.toString();
  return query ? `?${query}` : "";
}
