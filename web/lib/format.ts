/**
 * Value formatting driven by each metric's declared `scale` and `unit`
 * (plan issues 3.2, 3.5, 3.6, 3.7).
 *
 * The pipeline publishes 19 metrics on eight different scales. Formatting them
 * all the same way is how a dashboard ends up showing "0.1234567 proportion" and
 * "82.30000000000001 years". The rules live here, keyed on `scale`, so every
 * component prints the same value the same way and a new metric inherits a
 * sensible default instead of silently rendering raw floats.
 *
 * Locale is fixed to en-GB. This is a UK dataset with £ and a specific thousands
 * convention; letting the server and the browser disagree about locale would
 * also produce a hydration mismatch.
 */
import type { MetricCoverage, ScaleType } from "./types";

const LOCALE = "en-GB";

function nf(min: number, max = min): Intl.NumberFormat {
  return new Intl.NumberFormat(LOCALE, {
    minimumFractionDigits: min,
    maximumFractionDigits: max,
  });
}

const INTEGER = nf(0);
const ONE_DP = nf(1);
const TWO_DP = nf(2);
const GBP = new Intl.NumberFormat(LOCALE, {
  style: "currency",
  currency: "GBP",
  maximumFractionDigits: 0,
});
const PERCENT = new Intl.NumberFormat(LOCALE, {
  style: "percent",
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
});

/** A value formatted for display, without its unit. */
export function formatValue(value: number, scale: ScaleType): string {
  switch (scale) {
    case "currency":
      return GBP.format(value);
    case "count":
      return INTEGER.format(value);
    case "proportion":
      return PERCENT.format(value);
    case "standardised":
      // Sign carries the meaning here: a standardised domain is defined against
      // a zero mean, so "0.42" and "−0.42" must not look like the same value
      // with a stray character in front.
      return `${value > 0 ? "+" : value < 0 ? "−" : ""}${TWO_DP.format(Math.abs(value))}`;
    case "rating":
      return TWO_DP.format(value);
    case "rate":
    case "score":
    case "years":
      return ONE_DP.format(value);
    default:
      return ONE_DP.format(value);
  }
}

/**
 * Shorter form for axis ticks and the legend, where the column is narrow and the
 * exact figure is available elsewhere. Large counts become 1.2k / 3.4M.
 */
export function formatCompact(value: number, scale: ScaleType): string {
  if (scale === "count" || scale === "currency") {
    const abs = Math.abs(value);
    const prefix = scale === "currency" ? "£" : "";
    if (abs >= 1_000_000) return `${prefix}${ONE_DP.format(value / 1_000_000)}M`;
    if (abs >= 1_000) return `${prefix}${ONE_DP.format(value / 1_000)}k`;
    return `${prefix}${INTEGER.format(value)}`;
  }
  return formatValue(value, scale);
}

/**
 * Value with its unit, as a reader would say it. Currency and percentage already
 * carry their unit in the glyph, so repeating "GBP" or "proportion" after them
 * is noise.
 */
export function formatWithUnit(value: number, metric: MetricCoverage): string {
  const text = formatValue(value, metric.scale);
  if (metric.scale === "currency" || metric.scale === "proportion") return text;
  if (metric.scale === "standardised") return `${text} (${metric.unit})`;
  return `${text} ${metric.unit}`;
}

/** A signed difference, for the KPI deltas. Never "+0.0" for an exact zero. */
export function formatDelta(delta: number, scale: ScaleType): string {
  if (delta === 0) return "no change";
  const body = scale === "currency" ? GBP.format(Math.abs(delta)) : formatValue(Math.abs(delta), scale === "standardised" ? "rating" : scale);
  return `${delta > 0 ? "+" : "−"}${body}`;
}

/**
 * How a year should be spoken for a metric, given its `year_rule`. Pairing a
 * calendar year with a financial year or a three-year rolling period is a real
 * decision, so the label has to be able to say which it is.
 */
export function describeYear(year: number, metric: MetricCoverage): string {
  switch (metric.year_rule) {
    case "financial_start":
      return `${year}/${String((year + 1) % 100).padStart(2, "0")}`;
    case "rolling_end":
      return `${year - 2}–${year}`;
    case "snapshot":
      return `${year} snapshot`;
    case "calendar":
    default:
      return String(year);
  }
}

/** The one-line gloss of a year rule, for a footnote. */
export const YEAR_RULE_GLOSS: Record<MetricCoverage["year_rule"], string> = {
  calendar: "calendar year",
  financial_start: "financial year, labelled by its start",
  rolling_end: "three-year rolling period, labelled by its end year",
  snapshot: "a snapshot, not an annual series",
};
