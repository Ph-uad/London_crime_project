/**
 * Shared HTTP concerns for the data API: cache headers, query parsing and errors.
 */
import type { ApiError } from "./types";

/**
 * The exported data is immutable for the life of a deployment — it is bundled at
 * build time and only changes when the pipeline is re-run and the app redeployed.
 * So: never trusted in the browser, cached hard at the edge, and allowed to serve
 * stale for a day while revalidating.
 *
 * `immutable` is deliberately NOT used. It applies to the browser's `max-age`,
 * which is 0 here, so it would be meaningless at best and misleading at worst.
 */
export const IMMUTABLE_DATA_CACHE =
  "public, max-age=0, s-maxage=31536000, stale-while-revalidate=86400";

export function jsonResponse(
  body: unknown,
  init: { status?: number; contentType?: string; cache?: string } = {},
): Response {
  return new Response(JSON.stringify(body), {
    status: init.status ?? 200,
    headers: {
      "content-type": init.contentType ?? "application/json; charset=utf-8",
      "cache-control": init.cache ?? IMMUTABLE_DATA_CACHE,
    },
  });
}

/**
 * A 4xx with an actionable message. Errors are never cached — a client that fixes
 * its query must not be served its own mistake from the edge.
 */
export function errorResponse(status: number, body: ApiError): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

export class BadRequest extends Error {
  constructor(readonly body: ApiError) {
    super(body.error);
    this.name = "BadRequest";
  }
}

/** Truncate a list for an error message, so a 400 stays readable. */
function sample(values: readonly string[], max = 12): string[] {
  return values.length <= max ? [...values] : [...values.slice(0, max), `… +${values.length - max} more`];
}

/**
 * Reject unknown query parameters rather than ignoring them. A silently dropped
 * `?metrics=` (plural) returns the entire dataset and looks like it worked, which
 * is the same class of failure as a silent fallback in the pipeline.
 */
export function assertKnownParams(params: URLSearchParams, allowed: readonly string[]): void {
  const unknown = [...params.keys()].filter((k) => !allowed.includes(k));
  if (unknown.length) {
    throw new BadRequest({
      error: "Unknown query parameter",
      detail: `Not recognised: ${unknown.join(", ")}. This endpoint accepts ${allowed.join(", ")}.`,
      parameter: unknown[0],
      valid: [...allowed],
    });
  }
}

/** Comma-separated list, trimmed, blanks dropped. Absent → null (no filter). */
export function listParam(params: URLSearchParams, name: string): string[] | null {
  const raw = params.get(name);
  if (raw === null) return null;
  const values = raw
    .split(",")
    .map((v) => v.trim())
    .filter((v) => v.length > 0);
  if (!values.length) {
    throw new BadRequest({
      error: "Empty parameter",
      detail: `'${name}' was supplied but empty. Omit it to apply no filter on ${name}.`,
      parameter: name,
    });
  }
  return values;
}

/** Every value must be in `allowed`, otherwise 400 naming the offenders. */
export function assertAllowed(
  values: readonly string[],
  allowed: ReadonlySet<string> | readonly string[],
  name: string,
  hint: string,
): void {
  const set = allowed instanceof Set ? allowed : new Set(allowed);
  const bad = values.filter((v) => !set.has(v));
  if (bad.length) {
    throw new BadRequest({
      error: `Unknown ${name}`,
      detail: `${bad.join(", ")} — ${hint}`,
      parameter: name,
      valid: sample([...set].sort()),
    });
  }
}

/** Years as integers. Non-numeric or out-of-range values are a 400, not a silent NaN. */
export function yearParam(params: URLSearchParams, known: readonly number[]): number[] | null {
  const raw = listParam(params, "year");
  if (raw === null) return null;

  const bad = raw.filter((v) => !/^\d{4}$/.test(v));
  if (bad.length) {
    throw new BadRequest({
      error: "Invalid year",
      detail: `${bad.join(", ")} — years must be four digits, e.g. 2019. Use a comma-separated list for several.`,
      parameter: "year",
    });
  }

  const years = raw.map((v) => Number.parseInt(v, 10));
  const knownSet = new Set(known);
  const missing = years.filter((y) => !knownSet.has(y));
  if (missing.length) {
    throw new BadRequest({
      error: "Year not in the dataset",
      detail: `${missing.join(", ")} — the export covers ${Math.min(...known)}–${Math.max(...known)}. Coverage differs per metric; see /api/meta.`,
      parameter: "year",
      valid: known.map(String),
    });
  }
  return years;
}

/** Wrap a handler so a BadRequest becomes a 400 and anything else a 500. */
export async function handle(fn: () => Response | Promise<Response>): Promise<Response> {
  try {
    return await fn();
  } catch (err) {
    if (err instanceof BadRequest) return errorResponse(400, err.body);
    const detail = err instanceof Error ? err.message : String(err);
    return errorResponse(500, { error: "Internal error", detail });
  }
}
