/**
 * GET /api/metrics — the unified borough × year × metric × value dataset.
 * Plan issue 2.1.
 *
 * Filters (all optional, all comma-separated, all AND-ed):
 *   ?metric=crime_rate_per_1000,income_median
 *   ?year=2019,2020
 *   ?borough=E09000007
 *
 * The response carries the coverage entry for every metric it returns. That is
 * what keeps a client from mistaking a four-month 2026 for a year: the
 * `partial_years` flag travels with the data instead of requiring a second call
 * to /api/meta that a caller might skip.
 */
import {
  analysisWindow,
  allYears,
  boroughCodes,
  boroughs,
  boroughsExport,
  coverageFor,
  metricIds,
  selectObservations,
} from "@/lib/data";
import {
  assertAllowed,
  assertKnownParams,
  handle,
  jsonResponse,
  listParam,
  yearParam,
} from "@/lib/http";
import type { MetricsResponse } from "@/lib/types";

const PARAMS = ["metric", "year", "borough"] as const;

export function GET(request: Request): Promise<Response> {
  return handle(() => {
    const params = new URL(request.url).searchParams;
    assertKnownParams(params, PARAMS);

    const metric = listParam(params, "metric");
    if (metric) {
      assertAllowed(metric, metricIds, "metric", "see /api/meta for the metric list.");
    }

    const borough = listParam(params, "borough");
    if (borough) {
      assertAllowed(
        borough,
        boroughCodes,
        "borough",
        "boroughs are filtered by GSS code (e.g. E09000007), not by name.",
      );
    }

    const year = yearParam(params, allYears);

    const filters = { metric, year, borough };
    const rows = selectObservations(filters);

    const body: MetricsResponse = {
      generated_utc: boroughsExport.generated_utc,
      window: analysisWindow,
      filters,
      count: rows.length,
      boroughs: [...boroughs],
      coverage: coverageFor(rows),
      observations: rows,
    };
    return jsonResponse(body);
  });
}
