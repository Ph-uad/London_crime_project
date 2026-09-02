import "server-only";

import { readFileSync } from "node:fs";
import path from "node:path";

import { anchorFor, extentOf, fitExtent, pathFor } from "./projection";
import type { BoroughFeatureCollection } from "./types";

/**
 * Borough polygons — server-only.
 *
 * `london.geojson` cannot go through the `@data/*` alias: TypeScript and the
 * bundler treat only `.json` as a JSON module, and `.geojson` is not that. It is
 * read from disk instead, with `next.config.ts` tracing it into the deployment
 * bundle. Renaming the pipeline output would have been simpler, but
 * `london.geojson` is what issue 1.10's acceptance criteria name.
 *
 * The `server-only` import is the guard: reaching this from a client component
 * fails the build with that fact stated, rather than the chunking error a bare
 * `node:fs` import produces three modules downstream.
 */
export const GEOJSON_PATH = path.join(
  process.cwd(),
  "..",
  "data",
  "processed",
  "london.geojson",
);

let cache: BoroughFeatureCollection | null = null;

/**
 * Read lazily and cache, so a build that never calls /api/geo does not pay for
 * it, and a missing file surfaces as a clear API error rather than a crash at
 * module load.
 */
export function boroughGeoJson(): BoroughFeatureCollection {
  if (cache) return cache;
  cache = JSON.parse(readFileSync(GEOJSON_PATH, "utf8")) as BoroughFeatureCollection;
  return cache;
}

/**
 * The polygons projected into SVG path strings for the choropleth (issue 3.2).
 *
 * Projection happens on the server, once, for two reasons. The raw GeoJSON is
 * 171 KB of longitude/latitude pairs that the browser would have to project on
 * every render; and `lib/geo.ts` is `server-only`, so a client component cannot
 * reach the filesystem read that produces it. What crosses to the browser is 33
 * path strings in viewBox units — about half the bytes and none of the work.
 *
 * The join is on GSS code, not array position: `coverage.boroughs` is sorted by
 * name and the GeoJSON features are in code order. A positional join would
 * silently draw every borough with its neighbour's outline, which looks
 * plausible enough on a map of a city nobody in the room knows well. A borough
 * with no geometry is a hard failure — the pipeline already asserts the two
 * files agree, so reaching this means that assertion was bypassed.
 */
const VIEWBOX = { width: 1000, height: 720 } as const;
const PADDING = 8;

export interface BoroughShapes {
  shapes: string[];
  anchors: { x: number; y: number }[];
  viewBox: { width: number; height: number };
}

/**
 * Keyed by the borough list rather than holding a single entry: a one-slot cache
 * is thrashed by any caller that alternates between two lists, and then the
 * "cache" costs a projection of 6,587 vertices on every call while looking like
 * it is working. Production passes one list, so this holds one entry.
 */
const shapeCache = new Map<string, BoroughShapes>();

export function boroughShapes(order: readonly { gss: string; name: string }[]): BoroughShapes {
  const key = order.map((b) => b.gss).join(",");
  const hit = shapeCache.get(key);
  if (hit) return hit;

  const collection = boroughGeoJson();
  const byCode = new Map(collection.features.map((f) => [f.properties.borough_gss, f.geometry]));

  const missing = order.filter((b) => !byCode.has(b.gss));
  if (missing.length) {
    throw new Error(
      `No boundary geometry for ${missing.map((b) => `${b.name} (${b.gss})`).join(", ")}. ` +
        "boroughs.json and london.geojson disagree about which boroughs exist; re-run pipeline/03_borough_boundaries.R.",
    );
  }

  // Fit to the boroughs being drawn, not to the file's bbox, so the map fills
  // the frame whatever subset is passed.
  const geometries = order.map((b) => byCode.get(b.gss)!);
  const fit = fitExtent(extentOf(geometries), VIEWBOX.width, VIEWBOX.height, PADDING);

  const value: BoroughShapes = {
    shapes: geometries.map((g) => pathFor(g, fit)),
    anchors: geometries.map((g) => anchorFor(g, fit)),
    viewBox: { ...VIEWBOX },
  };
  shapeCache.set(key, value);
  return value;
}
