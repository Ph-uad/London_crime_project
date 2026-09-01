/**
 * The statistics the dashboard shows (plan issues 3.5, 3.6, 3.7).
 *
 * All of it is over 33 aggregated borough units, which is the constraint the
 * whole project is built around: these are associations between area averages,
 * and an association at borough level says nothing reliable about individuals.
 * The functions here therefore return the things needed to state that honestly —
 * n, how many units were dropped, and which years were actually paired — rather
 * than an r on its own.
 */
import type { Direction, MetricCoverage } from "./types";

/** Competition ranking: equal values share the better rank, and the next rank skips. */
export interface Rank {
  /** 1-based. */
  rank: number;
  /** The denominator: boroughs with a value, not 33 by default. */
  of: number;
  /** How many boroughs share this rank. */
  tied: number;
  /** True when rank 1 is the worst outcome rather than the highest value. */
  worstFirst: boolean;
}

/**
 * Rank one borough within a metric-year.
 *
 * The denominator is the count of boroughs that actually have a value, which is
 * 32 for well-being and life expectancy. Reporting "n/33" there would be a claim
 * about City of London that the source does not make — issue 3.5 calls this out
 * specifically.
 *
 * Rank 1 is the WORST outcome for a directional metric, so ranks mean the same
 * thing across metrics: 1st is the borough with the most crime and 1st is also
 * the borough with the lowest income. For a `neutral` metric there is no worse
 * end, so rank 1 is simply the highest value and the caller must say so.
 */
export function rankOf(
  values: readonly (number | null)[],
  index: number,
  direction: Direction,
): Rank | null {
  const self = values[index];
  if (self === null || self === undefined || !Number.isFinite(self)) return null;

  const present = values.filter((v): v is number => v !== null && Number.isFinite(v));
  const worstFirst = direction !== "neutral";
  // For higher_is_worse, worst = largest. For higher_is_better, worst = smallest.
  // For neutral, rank descending by value with no claim about quality.
  const descending = direction !== "higher_is_better";

  const better = present.filter((v) => (descending ? v > self : v < self)).length;
  const tied = present.filter((v) => v === self).length;

  return { rank: better + 1, of: present.length, tied, worstFirst };
}

/** "3rd of 32", with an ordinal suffix. */
export function ordinal(n: number): string {
  const mod100 = n % 100;
  if (mod100 >= 11 && mod100 <= 13) return `${n}th`;
  switch (n % 10) {
    case 1:
      return `${n}st`;
    case 2:
      return `${n}nd`;
    case 3:
      return `${n}rd`;
    default:
      return `${n}th`;
  }
}

export interface Summary {
  n: number;
  mean: number;
  min: { value: number; index: number };
  max: { value: number; index: number };
}

/**
 * Unweighted mean over the boroughs that have a value.
 *
 * Unweighted is a decision, not an oversight: a population-weighted London mean
 * would be a different quantity, dominated by the large outer boroughs, and the
 * unit of analysis throughout this project is the borough. The UI labels it as a
 * borough mean so it is not mistaken for a London-wide rate.
 */
export function summarise(values: readonly (number | null)[]): Summary | null {
  let n = 0;
  let total = 0;
  let min = { value: Infinity, index: -1 };
  let max = { value: -Infinity, index: -1 };

  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    if (v === null || v === undefined || !Number.isFinite(v)) continue;
    n++;
    total += v;
    if (v < min.value) min = { value: v, index: i };
    if (v > max.value) max = { value: v, index: i };
  }
  return n ? { n, mean: total / n, min, max } : null;
}

/** Which extreme is the bad one, so a card can be labelled by meaning. */
export function worstEnd(direction: Direction): "max" | "min" | null {
  if (direction === "higher_is_worse") return "max";
  if (direction === "higher_is_better") return "min";
  return null;
}

export interface Pair {
  index: number;
  x: number;
  y: number;
}

export interface Fit {
  slope: number;
  intercept: number;
  /** Pearson product-moment correlation. */
  r: number;
  n: number;
}

/**
 * Ordinary least squares plus Pearson r, in one pass over the pairs.
 *
 * Returns null rather than NaN when the fit is undefined — fewer than three
 * points, or no variance in one variable. `imd_employment_score` has a single
 * distinct value across all 33 boroughs, so a zero-variance x is a case that
 * occurs in this dataset, not a theoretical guard.
 */
export function fitLine(pairs: readonly Pair[]): Fit | null {
  const n = pairs.length;
  if (n < 3) return null;

  let sx = 0;
  let sy = 0;
  for (const p of pairs) {
    sx += p.x;
    sy += p.y;
  }
  const mx = sx / n;
  const my = sy / n;

  let sxx = 0;
  let syy = 0;
  let sxy = 0;
  for (const p of pairs) {
    const dx = p.x - mx;
    const dy = p.y - my;
    sxx += dx * dx;
    syy += dy * dy;
    sxy += dx * dy;
  }
  if (noVariance(sxx, mx, n) || noVariance(syy, my, n)) return null;

  const slope = sxy / sxx;
  return {
    slope,
    intercept: my - slope * mx,
    r: sxy / Math.sqrt(sxx * syy),
    n,
  };
}

/**
 * Whether a sum of squared deviations is indistinguishable from zero.
 *
 * `sxx === 0` is not the right test and it does not hold in practice. Thirty-three
 * identical values of 0.1 have a mean of 0.10000000000000002, so each deviation
 * is about −1.4e-17 rather than 0 and the sum is ~5.8e-34: small, but non-zero,
 * and enough for the division below to return a slope and an r built entirely
 * out of rounding error. `imd_employment_score` is exactly this — one distinct
 * value across all 33 boroughs — so a scatter against it would have shown a
 * fitted line and a correlation coefficient with no data behind either.
 *
 * The tolerance is relative to the values themselves: identical inputs of
 * magnitude m leave a residue of order n·(m·ε)². Real data clears it by orders
 * of magnitude — the IMD living-environment scores differ by whole points on a
 * mean of 25, giving a sum around 0.5 against a threshold near 1e-27.
 */
function noVariance(sumSquares: number, mean: number, n: number): boolean {
  if (sumSquares === 0) return true;
  const scale = Math.max(Math.abs(mean), Number.MIN_VALUE);
  return sumSquares <= 16 * n * (scale * Number.EPSILON) ** 2;
}

/** A conventional, deliberately unassertive gloss on |r|. */
export function describeStrength(r: number): string {
  const a = Math.abs(r);
  if (a >= 0.7) return "strong";
  if (a >= 0.5) return "moderate";
  if (a >= 0.3) return "weak";
  return "very weak";
}

export interface Change {
  from: number;
  to: number;
  fromYear: number;
  toYear: number;
  delta: number;
  /** Whether the change moved towards the good end. Null for a neutral metric. */
  improved: boolean | null;
}

/**
 * Long-run change across a metric's own series.
 *
 * Endpoints skip years listed in `partial_years`: `crime_count` runs to 2026,
 * but 2026 is four months of data, and using it as the endpoint of a fifteen-year
 * trend would report a collapse in crime that is really a collapse in coverage.
 */
export function longRunChange(
  metric: MetricCoverage,
  valueOf: (year: number) => number | null,
): Change | null {
  // A snapshot is not a series. IMD 2015 and IMD 2019 are separate exercises —
  // the pipeline already drops their ranks because the two are not
  // methodologically comparable, and the scores inherit enough of that to make
  // "deprivation fell by 0.18" a sentence about the index rather than about
  // London. Cross-sectional comparison is what two snapshots support.
  if (metric.cadence === "snapshot") return null;

  const partial = new Set(metric.partial_years);
  const usable = metric.years.filter((y) => !partial.has(y)).sort((a, b) => a - b);

  let first: { year: number; value: number } | null = null;
  let last: { year: number; value: number } | null = null;
  for (const y of usable) {
    const v = valueOf(y);
    if (v === null || !Number.isFinite(v)) continue;
    first ??= { year: y, value: v };
    last = { year: y, value: v };
  }
  if (!first || !last || first.year === last.year) return null;

  const delta = last.value - first.value;
  const end = worstEnd(metric.direction);
  return {
    from: first.value,
    to: last.value,
    fromYear: first.year,
    toYear: last.year,
    delta,
    // Falling crime is an improvement; falling life expectancy is not. The sign
    // alone cannot tell you which, which is why direction is in the contract.
    improved: end === null ? null : delta === 0 ? null : end === "max" ? delta < 0 : delta > 0,
  };
}
