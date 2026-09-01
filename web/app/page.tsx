import { Dashboard } from "@/components/dashboard/dashboard";
import { boroughs, coverage, observations } from "@/lib/data";
import { boroughShapes } from "@/lib/geo";
import { buildSeries, type DashboardData } from "@/lib/series";
import { parseState } from "@/lib/url-state";
import { FACTS, SITE } from "@/lib/site";

/**
 * The dashboard route (plan issues 3.1–3.7).
 *
 * A server component that does three things and then gets out of the way:
 *
 *   1. Projects the borough polygons into SVG paths. `lib/geo.ts` is
 *      `server-only` — it reads the filesystem — so this is the last point at
 *      which the geometry can be touched.
 *   2. Compacts 6,001 observations into an index keyed by metric, year and
 *      borough position. The raw export is 516 KB; the index is a fraction of
 *      that and the browser never sees the rest.
 *   3. Parses the query string into the initial state, so a shared link renders
 *      correctly in the first HTML instead of flashing the default view.
 *
 * Reading `searchParams` makes this route dynamic. That is the honest shape for
 * a parameterised dashboard, and the underlying data is still bundled at build
 * time, so there is no per-request I/O beyond the parse.
 */
export default async function DashboardPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = await searchParams;
  const { state, rejected } = parseState(params, coverage);

  const geometry = boroughShapes(boroughs);
  const data: DashboardData = {
    generatedUtc: coverage.generated_utc,
    boroughs: [...boroughs],
    metrics: coverage.metrics,
    series: buildSeries(observations, boroughs),
    shapes: geometry.shapes,
    anchors: geometry.anchors,
    viewBox: geometry.viewBox,
    window: coverage.window,
  };

  const annual = Object.values(coverage.metrics).filter((m) => m.cadence === "annual").length;

  return (
    <div className="mx-auto max-w-[1280px] px-4 py-6 sm:px-6 sm:py-8">
      <h1 className="max-w-3xl text-2xl font-semibold tracking-tight text-[var(--text-primary)] sm:text-3xl">
        {SITE.name}
      </h1>
      <p className="mt-2 max-w-prose text-[var(--text-secondary)]">{SITE.tagline}.</p>
      <p className="mt-1 text-xs text-[var(--text-secondary)]">
        <span className="tabular-nums">{FACTS.boroughs}</span> boroughs ·{" "}
        <span className="tabular-nums">{FACTS.metrics}</span> metrics (
        <span className="tabular-nums">{annual}</span> annual series and{" "}
        <span className="tabular-nums">{FACTS.metrics - annual}</span> deprivation snapshots) ·
        cross-metric window <span className="tabular-nums">{FACTS.analysisStart}–{FACTS.analysisEnd}</span>,
        crime trend to <span className="tabular-nums">{FACTS.trendEnd}</span>
      </p>

      <div className="mt-6">
        <Dashboard data={data} coverage={coverage} initial={state} rejected={rejected} />
      </div>
    </div>
  );
}
