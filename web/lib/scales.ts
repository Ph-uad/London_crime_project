/**
 * Colour scales for the choropleth (plan issue 3.2).
 *
 * Three rules from the coverage matrix drive everything here, and none of them
 * can be guessed from the values alone:
 *
 *   scale       standardised metrics are defined against a zero mean, so they
 *               get a diverging ramp centred on zero. Everything else gets the
 *               sequential ramp. A single ramp across both would colour "0.4
 *               above average" the same as "0.4 units of something".
 *   direction   which end of the ramp is dark. The map reads DARKER = WORSE for
 *               every metric, so a reader comparing crime against income does
 *               not have to relearn the ramp between them.
 *   coverage    a borough the metric does not cover is drawn in the no-data
 *               style. Never omitted, never zero.
 *
 * Every colour is a token reference or a `color-mix` over token references.
 * There is no hex in this file, which is the rule `app/globals.css` sets for
 * everything from 3.2 onwards.
 *
 * CLASSING. Sequential metrics use quantile breaks, not equal intervals,
 * because n = 33 and the distributions are not symmetric: City of London's crime
 * rate is 698 per 1,000 against a median of 113, so seven equal intervals put 32
 * boroughs in the lowest class and produce a map of one red dot. Quantiles cost
 * magnitude — four classes of eight boroughs look evenly spaced whatever the
 * gaps between them — so the legend prints the real break values, and issue
 * 3.3's borough exclusion exists to let a reader drop the outlier and see the
 * rest on their own terms. Diverging metrics do NOT use quantiles: a quantile
 * break would move the midpoint off zero, which is the one thing a diverging
 * ramp is for.
 */
import type { Direction, MetricCoverage, ScaleType } from "./types";
import { formatValue } from "./format";

export type Ramp = "sequential" | "diverging";

/** The fill for a borough this metric does not cover. See NO_DATA_PATTERN_ID. */
export const NO_DATA_FILL = "url(#no-data-hatch)";

/**
 * A hatch as well as a colour. Issue 3.8 requires the no-data state to be
 * distinguishable without relying on colour alone, and a grey between a pale
 * ramp step and a dark one is exactly the case where colour alone fails.
 */
export const NO_DATA_PATTERN_ID = "no-data-hatch";

/** The seven sequential steps, lightest to darkest. */
const SEQUENTIAL = [
  "var(--seq-100)",
  "var(--seq-200)",
  "var(--seq-300)",
  "var(--seq-400)",
  "var(--seq-500)",
  "var(--seq-600)",
  "var(--seq-700)",
] as const;

/**
 * Diverging steps built from the three poles by mixing in oklab, which keeps
 * lightness moving evenly — mixing in sRGB puts a muddy band either side of the
 * midpoint. Index 3 is the midpoint, which sits at zero.
 */
function divergingSteps(negativePole: string, positivePole: string): string[] {
  const mix = (pole: string, pct: number) =>
    `color-mix(in oklab, var(${pole}) ${pct}%, var(--div-mid))`;
  return [
    `var(${negativePole})`,
    mix(negativePole, 66),
    mix(negativePole, 33),
    "var(--div-mid)",
    mix(positivePole, 33),
    mix(positivePole, 66),
    `var(${positivePole})`,
  ];
}

export interface ColourClass {
  fill: string;
  /** Inclusive lower bound in data units. */
  min: number;
  /** Upper bound; inclusive only for the last class. */
  max: number;
}

export interface ColourScale {
  ramp: Ramp;
  classes: ColourClass[];
  /** [min, max] of the values the scale was built from, or null if there were none. */
  domain: [number, number] | null;
  /** How many values went into it, after exclusions. */
  n: number;
  /** True when every value is identical, so the map carries no information. */
  degenerate: boolean;
  /** One sentence telling a reader how to read the ramp. */
  note: string;
  /** The fill for a value, or the no-data hatch for null/non-finite. */
  fillOf(value: number | null | undefined): string;
  /** Index into `classes`, or -1. */
  classOf(value: number): number;
}

const CLASS_COUNT = 7;

/**
 * Type-7 quantile, the R and NumPy default. Written out rather than pulled from
 * d3-scale: it is four lines, and the alternative is a dependency whose other
 * 40 kB we would not use.
 */
function quantile(sorted: readonly number[], p: number): number {
  if (sorted.length === 1) return sorted[0];
  const h = (sorted.length - 1) * p;
  const lo = Math.floor(h);
  const hi = Math.ceil(h);
  return sorted[lo] + (h - lo) * (sorted[hi] - sorted[lo]);
}

/**
 * Quantile breaks, collapsing to fewer classes when the data cannot support
 * seven. `imd_employment_score` is published to one decimal place and has a
 * single distinct value across all 33 boroughs; forcing seven classes onto it
 * would invent six boundaries the source does not contain.
 */
function quantileBreaks(sorted: readonly number[], k: number): number[] {
  const breaks: number[] = [];
  for (let i = 1; i < k; i++) {
    const b = quantile(sorted, i / k);
    if (!breaks.length || b > breaks[breaks.length - 1]) breaks.push(b);
  }
  return breaks;
}

/**
 * Whether the ramp runs light-to-dark with increasing value.
 *
 * DARKER = WORSE is the convention. So a `higher_is_worse` metric runs
 * low → light, and a `higher_is_better` metric runs low → dark. `neutral`
 * metrics have no worse end, so they keep the conventional dark = more and the
 * legend says so instead of claiming a direction the data does not have.
 */
function darkIsHigh(direction: Direction): boolean {
  return direction !== "higher_is_better";
}

function sequentialNote(metric: MetricCoverage): string {
  if (metric.direction === "neutral") {
    return "Darker means a higher value. This metric has no better or worse direction.";
  }
  const worseEnd = metric.direction === "higher_is_worse" ? "higher" : "lower";
  return `Darker means ${worseEnd} — the worse end for this metric.`;
}

function divergingNote(metric: MetricCoverage): string {
  const redEnd = metric.direction === "higher_is_better" ? "below" : "above";
  return `Centred on zero. Red is ${redEnd} the national average — the worse end for this metric — and blue is the other side. The midpoint is zero, not the middle of London's range.`;
}

/**
 * Build the scale for one metric over one set of values.
 *
 * `values` is already filtered: nulls removed and any boroughs excluded by the
 * 3.3 control dropped. Passing them in rather than reading the series here keeps
 * this function pure and lets the same code build the legend, the map and the
 * scatter's colour key from identical inputs.
 */
export function buildColourScale(
  metric: MetricCoverage,
  values: readonly (number | null)[],
): ColourScale {
  const clean = values.filter((v): v is number => v !== null && Number.isFinite(v));
  const sorted = [...clean].sort((a, b) => a - b);
  const n = sorted.length;

  if (!n) {
    return emptyScale("No values for this metric and year, so there is nothing to colour.");
  }

  const lo = sorted[0];
  const hi = sorted[n - 1];
  const degenerate = lo === hi;

  if (metric.scale === "standardised") {
    return divergingScale(metric, lo, hi, n, degenerate);
  }
  return sequentialScale(metric, sorted, lo, hi, n, degenerate);
}

function emptyScale(note: string): ColourScale {
  return {
    ramp: "sequential",
    classes: [],
    domain: null,
    n: 0,
    degenerate: true,
    note,
    fillOf: () => NO_DATA_FILL,
    classOf: () => -1,
  };
}

function sequentialScale(
  metric: MetricCoverage,
  sorted: readonly number[],
  lo: number,
  hi: number,
  n: number,
  degenerate: boolean,
): ColourScale {
  const breaks = degenerate ? [] : quantileBreaks(sorted, CLASS_COUNT);
  const k = breaks.length + 1;

  // Take the k steps that span the full ramp, so a 3-class map still runs from
  // the palette's lightest to its darkest rather than stopping a third of the
  // way along.
  const steps = spread(SEQUENTIAL, k);
  const ordered = darkIsHigh(metric.direction) ? steps : [...steps].reverse();

  const bounds = [lo, ...breaks, hi];
  const classes: ColourClass[] = ordered.map((fill, i) => ({
    fill,
    min: bounds[i],
    max: bounds[i + 1],
  }));

  return finish(
    "sequential",
    classes,
    [lo, hi],
    n,
    degenerate,
    degenerate
      ? degenerateNote(metric, lo)
      : sequentialNote(metric) +
          ` Classes are quantiles of the ${n} boroughs shown, so each holds a similar count; the break values below give the magnitudes.`,
  );
}

function divergingScale(
  metric: MetricCoverage,
  lo: number,
  hi: number,
  n: number,
  degenerate: boolean,
): ColourScale {
  // Symmetric about zero: the extent is set by whichever side reaches furthest,
  // so a class the same distance either side of zero is the same distance in
  // colour. An asymmetric domain would put the neutral midpoint off zero.
  const extent = Math.max(Math.abs(lo), Math.abs(hi)) || 1;
  const step = (extent * 2) / CLASS_COUNT;

  const steps =
    metric.direction === "higher_is_better"
      ? divergingSteps("--div-high", "--div-low")
      : divergingSteps("--div-low", "--div-high");

  const classes: ColourClass[] = steps.map((fill, i) => ({
    fill,
    min: -extent + step * i,
    max: -extent + step * (i + 1),
  }));

  return finish(
    "diverging",
    classes,
    [lo, hi],
    n,
    degenerate,
    degenerate ? degenerateNote(metric, lo) : divergingNote(metric),
  );
}

function degenerateNote(metric: MetricCoverage, value: number): string {
  return `Every borough shown has the same value (${formatValue(value, metric.scale)}), so the map is one flat colour. That is what the source publishes at this precision, not a rendering fault — see the methodology note on IMD score precision.`;
}

/** Pick k evenly spaced entries from a ramp, always including both ends. */
function spread(ramp: readonly string[], k: number): string[] {
  // One class means every borough has the same value. Painting them all in the
  // ramp's darkest step would say "everywhere is the worst"; the middle step
  // says "no variation", which is what the data says.
  if (k <= 1) return [ramp[Math.floor(ramp.length / 2)]];
  if (k >= ramp.length) return [...ramp];
  return Array.from({ length: k }, (_, i) =>
    ramp[Math.round((i * (ramp.length - 1)) / (k - 1))],
  );
}

function finish(
  ramp: Ramp,
  classes: ColourClass[],
  domain: [number, number],
  n: number,
  degenerate: boolean,
  note: string,
): ColourScale {
  const classOf = (value: number): number => {
    if (!Number.isFinite(value) || !classes.length) return -1;
    for (let i = 0; i < classes.length - 1; i++) {
      if (value < classes[i].max) return i;
    }
    return classes.length - 1;
  };

  return {
    ramp,
    classes,
    domain,
    n,
    degenerate,
    note,
    classOf,
    fillOf(value) {
      if (value === null || value === undefined || !Number.isFinite(value)) {
        return NO_DATA_FILL;
      }
      const i = classOf(value);
      return i < 0 ? NO_DATA_FILL : classes[i].fill;
    },
  };
}

/**
 * Axis ticks at round numbers (plan issue 3.6).
 *
 * The 1-2-5 rule: pick the step from that sequence times a power of ten that
 * gets closest to the requested tick count, then walk the multiples of it that
 * fall inside the domain. Ticks at 0, 25, 50 read; ticks at 0, 23.7, 47.4 do
 * not, and a reader cannot estimate a point's value from an axis they have to
 * do arithmetic on.
 */
export function niceTicks(min: number, max: number, count = 5): number[] {
  if (!Number.isFinite(min) || !Number.isFinite(max)) return [];
  if (min === max) return [min];
  if (min > max) [min, max] = [max, min];

  const rough = (max - min) / Math.max(1, count);
  const magnitude = 10 ** Math.floor(Math.log10(rough));
  const step =
    [1, 2, 5, 10].map((m) => m * magnitude).find((s) => s >= rough) ?? 10 * magnitude;

  const ticks: number[] = [];
  const first = Math.ceil(min / step) * step;
  for (let t = first; t <= max + step / 1e9; t += step) {
    // Re-round each tick: accumulating a float step drifts (0.1+0.1+0.1).
    ticks.push(Math.round(t / step) * step);
  }
  return ticks;
}

/** Legend rows: the fill plus the range it stands for, already formatted. */
export function legendRows(
  scale: ColourScale,
  metricScale: ScaleType,
): { fill: string; label: string }[] {
  return scale.classes.map((c, i) => ({
    fill: c.fill,
    label:
      i === scale.classes.length - 1
        ? `${formatValue(c.min, metricScale)} – ${formatValue(c.max, metricScale)}`
        : `${formatValue(c.min, metricScale)} – <${formatValue(c.max, metricScale)}`,
  }));
}
