/**
 * Site-level facts derived from pipeline output, so nothing here is hand-typed
 * and able to drift from what the data actually says.
 *
 * Server components import from `@/lib/coverage` rather than fetching
 * `/api/meta`. It is the same source the API route serves — fetching your own
 * route from a server component adds a round trip and an absolute-URL problem
 * for no gain. The HTTP contract itself is covered by the route tests.
 *
 * The import is `lib/coverage`, NOT `lib/data`: this module is reached from
 * `components/site-header.tsx`, which is a client component, so anything it
 * touches ends up in the browser bundle. `lib/coverage` is the 9 KB matrix;
 * `lib/data` is 516 KB of observations behind a `server-only` marker.
 */
import { boroughs, coverage, metricIds } from "@/lib/coverage";
import { metricFamily } from "@/lib/series";

export const NAV = [
  { href: "/", label: "Dashboard" },
  { href: "/insights", label: "Insights" },
  { href: "/methodology", label: "Methodology" },
] as const;

export const SITE = {
  name: "London Crime & Social Determinants",
  short: "Crime × Determinants",
  tagline: "How recorded crime in London associates with income, deprivation, well-being and life expectancy",
} as const;

/** Real counts, read from the coverage matrix at build time. */
export const FACTS = {
  boroughs: boroughs.length,
  metrics: metricIds.length,
  analysisStart: coverage.window.analysis_start,
  analysisEnd: coverage.window.analysis_end,
  trendEnd: coverage.window.trend_end,
  generated: coverage.generated_utc,
};

/** Distinct publishers, taken from each metric's `source` string. */
export const ATTRIBUTIONS: string[] = Array.from(
  new Set(Object.values(coverage.metrics).map((m) => m.source)),
).sort();

/**
 * Boroughs that some metrics do not cover, grouped by borough rather than by
 * metric. Eight separate lines all ending "excludes City of London" is noise;
 * one line naming the borough and the metric families is the information.
 *
 * Surfaced in the footer because a 32-vs-33 difference is exactly the kind of
 * thing a reader assumes away unless told.
 */
export const PARTIAL_COVERAGE = Object.values(
  Object.entries(coverage.metrics)
    .filter(([, m]) => m.boroughs_missing.length > 0)
    .reduce<Record<string, { borough: string; families: Set<string> }>>((acc, [id, m]) => {
      for (const b of m.boroughs_missing) {
        acc[b.gss] ??= { borough: b.name, families: new Set() };
        acc[b.gss].families.add(metricFamily(id));
      }
      return acc;
    }, {}),
).map((g) => ({ borough: g.borough, families: [...g.families].sort() }));
