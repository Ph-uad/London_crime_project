/**
 * The compact borough × year × metric index the dashboard runs on
 * (plan issues 3.2–3.7).
 *
 * `boroughs.json` is 516 KB of one-object-per-observation. Sending that to the
 * browser to look up 33 values would be absurd, and importing it from a client
 * component would put the whole file in the client bundle. Instead the server
 * builds this index once and passes it down as a prop:
 *
 *     metric → year → value[boroughIndex]
 *
 * Positions align with the `boroughs` array, so a lookup is two object hops and
 * an array index rather than a scan of 6,001 rows, and the payload drops to
 * roughly a sixth of the raw export.
 *
 * `null` means "no value", and every hole is explicit — a missing borough is a
 * null in its slot, never a short array or an absent key. Anything that renders
 * a value has to decide what to do about the null, which is the point: City of
 * London has no well-being figure and must not quietly become a zero.
 */
import type {
  BoroughRef,
  CoverageMatrix,
  MetricCoverage,
  Observation,
} from "./types";

/** metric → year → value per borough index. */
export type SeriesIndex = Record<string, Record<number, (number | null)[]>>;

/** Everything the client half of the dashboard needs, and nothing else. */
export interface DashboardData {
  generatedUtc: string;
  boroughs: BoroughRef[];
  metrics: Record<string, MetricCoverage>;
  series: SeriesIndex;
  /** SVG path per borough, index-aligned with `boroughs`. */
  shapes: string[];
  /** Label anchor per borough, in the same viewBox. */
  anchors: { x: number; y: number }[];
  viewBox: { width: number; height: number };
  window: CoverageMatrix["window"];
}

export function buildSeries(
  observations: readonly Observation[],
  boroughs: readonly BoroughRef[],
): SeriesIndex {
  const slot = new Map(boroughs.map((b, i) => [b.gss, i]));
  const out: SeriesIndex = {};

  for (const o of observations) {
    const i = slot.get(o.borough_gss);
    if (i === undefined) {
      // The pipeline asserts every observation carries one of the 33 codes, so
      // this cannot happen against real output. If it ever does, a silent drop
      // would hide a broken join behind a map that merely looks a bit pale.
      throw new Error(
        `Observation for unknown borough '${o.borough_gss}' (metric ${o.metric}, year ${o.year}).`,
      );
    }
    const byYear = (out[o.metric] ??= {});
    const row = (byYear[o.year] ??= new Array<number | null>(boroughs.length).fill(null));
    row[i] = o.value;
  }
  return out;
}

/** The values for one metric-year, or an all-null row if that year has none. */
export function rowFor(
  series: SeriesIndex,
  metric: string,
  year: number,
  size: number,
): (number | null)[] {
  return series[metric]?.[year] ?? new Array<number | null>(size).fill(null);
}

export function valueAt(
  series: SeriesIndex,
  metric: string,
  year: number,
  boroughIndex: number,
): number | null {
  return series[metric]?.[year]?.[boroughIndex] ?? null;
}

/**
 * The year of `metric` closest to `target`, or null if the metric has no years.
 *
 * Ties break towards the earlier year, so a pairing is never silently moved
 * forward into a year the reader has not selected. This is a *pairing* rule, not
 * interpolation — the value returned is the one actually published for that
 * year, and the caller is expected to print which year it used.
 */
export function nearestYear(coverage: MetricCoverage, target: number): number | null {
  let best: number | null = null;
  let bestGap = Infinity;
  for (const y of coverage.years) {
    const gap = Math.abs(y - target);
    if (gap < bestGap || (gap === bestGap && best !== null && y < best)) {
      best = y;
      bestGap = gap;
    }
  }
  return best;
}

/** The year of `metric` nearest `target` that is not flagged partial. */
export function nearestCompleteYear(coverage: MetricCoverage, target: number): number | null {
  const partial = new Set(coverage.partial_years);
  const usable = coverage.years.filter((y) => !partial.has(y));
  if (!usable.length) return null;
  return nearestYear({ ...coverage, years: usable }, target);
}

/**
 * Why a borough-year has no value. Issue 3.5 requires that an empty cell say
 * which of these it is, because they mean completely different things: a metric
 * that does not exist for a borough is a property of the source, a year outside
 * the range is a property of the series, and a hole inside the range is a
 * property of that one observation.
 */
export type Absence =
  | { kind: "not_covered"; text: string }
  | { kind: "outside_series"; text: string }
  | { kind: "no_observation"; text: string };

export function absenceReason(
  metric: MetricCoverage,
  metricLabel: string,
  year: number,
  borough: BoroughRef,
): Absence {
  if (metric.boroughs_missing.some((b) => b.gss === borough.gss)) {
    return {
      kind: "not_covered",
      text: `Not published for ${borough.name} — the resident population is too small for a reliable estimate.`,
    };
  }
  if (!metric.years.includes(year)) {
    const lo = Math.min(...metric.years);
    const hi = Math.max(...metric.years);
    return {
      kind: "outside_series",
      text:
        metric.cadence === "snapshot"
          ? `${metricLabel} exists only for ${metric.years.join(" and ")}, not ${year}.`
          : `${metricLabel} runs ${lo}–${hi}; ${year} is outside it.`,
    };
  }
  return {
    kind: "no_observation",
    text: `No value published for ${borough.name} in ${year}.`,
  };
}

/**
 * The family a metric belongs to, derived from its id prefix.
 *
 * Used to group the metric switcher and to say "City of London has no
 * well-being or life expectancy data" once instead of eight times. Lives here
 * rather than in `lib/site.ts` because this module imports nothing but types, so
 * a client component can reach it without pulling the whole data export into the
 * browser bundle.
 */
export function metricFamily(metricId: string): string {
  if (metricId.startsWith("wellbeing_")) return "well-being";
  if (metricId.startsWith("life_expectancy_")) return "life expectancy";
  if (metricId.startsWith("imd_")) return "deprivation";
  if (metricId.startsWith("income_")) return "income";
  if (metricId.startsWith("crime_")) return "crime";
  return metricId;
}

/** Display order for the families, outcome first then the determinants. */
export const FAMILY_ORDER = [
  "crime",
  "income",
  "deprivation",
  "well-being",
  "life expectancy",
] as const;

/** Metric ids grouped by family, in FAMILY_ORDER, each group sorted by label. */
export function groupByFamily(
  metrics: Record<string, MetricCoverage>,
): { family: string; ids: string[] }[] {
  const groups = new Map<string, string[]>();
  for (const id of Object.keys(metrics)) {
    const f = metricFamily(id);
    (groups.get(f) ?? groups.set(f, []).get(f)!).push(id);
  }
  const known = FAMILY_ORDER.filter((f) => groups.has(f));
  const rest = [...groups.keys()].filter((f) => !FAMILY_ORDER.includes(f as never)).sort();
  return [...known, ...rest].map((family) => ({
    family,
    ids: groups.get(family)!.sort((a, b) => metrics[a].label.localeCompare(metrics[b].label)),
  }));
}

/** Distinct sorted years across a set of metrics — the union, not any one range. */
export function unionYears(
  metrics: Record<string, MetricCoverage>,
  ids: readonly string[],
): number[] {
  const seen = new Set<number>();
  for (const id of ids) for (const y of metrics[id]?.years ?? []) seen.add(y);
  return [...seen].sort((a, b) => a - b);
}
