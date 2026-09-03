/**
 * GET /api/geo : the 33 borough polygons, EPSG:4326 / RFC 7946.
 * Plan issue 2.2.
 *
 * Served verbatim from pipeline output with the GeoJSON media type. The GSS codes
 * in `properties.borough_gss` are asserted against boroughs.json by
 * pipeline/03_borough_boundaries.R, so the map and the data cannot disagree about
 * which boroughs exist.
 */
import { boroughGeoJson, GEOJSON_PATH } from "@/lib/geo";
import { errorResponse, handle, jsonResponse } from "@/lib/http";

export function GET(request: Request): Promise<Response> {
  return handle(() => {
    const params = new URL(request.url).searchParams;
    const unknown = [...params.keys()];
    if (unknown.length) {
      return errorResponse(400, {
        error: "Unknown query parameter",
        detail: `Not recognised: ${unknown.join(", ")}. This endpoint takes no parameters.`,
        parameter: unknown[0],
      });
    }

    try {
      return jsonResponse(boroughGeoJson(), {
        contentType: "application/geo+json; charset=utf-8",
      });
    } catch {
      // A missing file here means the pipeline has not been run, which is an
      // operational fact worth stating plainly rather than a generic 500.
      return errorResponse(503, {
        error: "Boundary data not available",
        detail: `Could not read ${GEOJSON_PATH}. Run 'Rscript pipeline/03_borough_boundaries.R' to generate it.`,
      });
    }
  });
}
