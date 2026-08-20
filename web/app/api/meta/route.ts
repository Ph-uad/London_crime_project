/**
 * GET /api/meta — the coverage matrix.
 * Plan issue 2.1 (amendment).
 *
 * This is the contract that lets the frontend stop guessing: per metric it
 * declares the years that exist, which of them are partial, whether the metric is
 * annual or a snapshot, which direction is "good", what scale the values sit on,
 * how the year was derived, and which boroughs it does not cover.
 *
 * Served whole. Filtering it would invite a client to fetch only the fields it
 * currently uses and then infer the rest, which is the failure this endpoint
 * exists to prevent.
 */
import { coverage } from "@/lib/data";
import { assertKnownParams, handle, jsonResponse } from "@/lib/http";

export function GET(request: Request): Promise<Response> {
  return handle(() => {
    assertKnownParams(new URL(request.url).searchParams, []);
    return jsonResponse(coverage);
  });
}
