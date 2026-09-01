/**
 * Web Mercator projection and SVG path building for the borough choropleth
 * (plan issue 3.2).
 *
 * WHY THIS IS HAND-WRITTEN RATHER THAN MapLibre GL + d3-geo, which the plan
 * named as the likely technologies:
 *
 *   The map is 33 static polygons with no basemap. MapLibre's value is tiles,
 *   labels and a style pipeline; using it here means either a third-party tile
 *   endpoint — a network dependency, an API key and an attribution obligation
 *   this project does not otherwise carry — or a style with no basemap, which is
 *   ~900 KB of WebGL to fill polygons. It also renders into a canvas, which is
 *   one opaque node to a screen reader and to axe, and which Playwright cannot
 *   assert on without pixel diffing. Issue 3.8 asks for a measured accessibility
 *   pass, so an SVG the tests can read is worth more than a GPU we do not need.
 *
 *   The cost, stated plainly: no basemap context and no street detail. Pan and
 *   zoom are implemented on the SVG viewBox instead, so 3.2's touch criterion is
 *   met, but a reader cannot zoom in to see roads. For a borough-level
 *   choropleth that is the right trade; for a point map of individual crimes it
 *   would not be.
 *
 * Web Mercator (EPSG:3857) is used rather than a UK projection because the
 * source is EPSG:4326 and the extent is 0.84° wide. Nothing here is a
 * simplification: the formulae are exact, and the reason a plate carrée would
 * have been *visually* acceptable at this latitude is precisely why using the
 * real thing costs nothing.
 */

/** A longitude/latitude pair as GeoJSON stores it: [x, y] = [lon, lat]. */
export type LonLat = readonly [number, number];

/** Unitless Web Mercator plane coordinates; y grows northward. */
export interface Mercator {
  x: number;
  y: number;
}

/** Latitudes beyond this project to infinity. London is nowhere near it. */
const MAX_LAT = 85.05112878;

export function project([lon, lat]: LonLat): Mercator {
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) {
    throw new RangeError(`Non-finite coordinate: [${lon}, ${lat}]`);
  }
  if (Math.abs(lat) > MAX_LAT) {
    throw new RangeError(`Latitude ${lat} is outside the Mercator domain (±${MAX_LAT}).`);
  }
  const rad = (lat * Math.PI) / 180;
  return { x: lon, y: (180 / Math.PI) * Math.log(Math.tan(Math.PI / 4 + rad / 2)) };
}

export interface Extent {
  minX: number;
  minY: number;
  maxX: number;
  maxY: number;
}

/**
 * A transform from Mercator space into an SVG viewBox of the given size,
 * preserving aspect ratio and centring the content. SVG y grows downward, so the
 * north-up flip happens here rather than being left to a scale(-1) on the group.
 */
export interface Fit {
  width: number;
  height: number;
  scale: number;
  offsetX: number;
  offsetY: number;
}

export function fitExtent(extent: Extent, width: number, height: number, padding = 0): Fit {
  const spanX = extent.maxX - extent.minX;
  const spanY = extent.maxY - extent.minY;
  if (!(spanX > 0) || !(spanY > 0)) {
    throw new RangeError("Extent has no area; cannot fit it to a box.");
  }
  const usableW = width - padding * 2;
  const usableH = height - padding * 2;
  if (!(usableW > 0) || !(usableH > 0)) {
    throw new RangeError("Padding leaves no room inside the box.");
  }

  const scale = Math.min(usableW / spanX, usableH / spanY);
  // Centre the projected content in whichever axis has slack.
  const offsetX = padding + (usableW - spanX * scale) / 2 - extent.minX * scale;
  const offsetY = padding + (usableH - spanY * scale) / 2 + extent.maxY * scale;
  return { width, height, scale, offsetX, offsetY };
}

export function applyFit(p: Mercator, fit: Fit): { x: number; y: number } {
  return { x: p.x * fit.scale + fit.offsetX, y: fit.offsetY - p.y * fit.scale };
}

/** GeoJSON ring/polygon nesting, kept loose because the source is external. */
type Ring = LonLat[];
type PolygonCoords = Ring[];
type MultiPolygonCoords = PolygonCoords[];

export function extentOf(
  geometries: readonly { type: string; coordinates: unknown }[],
): Extent {
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;

  for (const g of geometries) {
    for (const ring of ringsOf(g)) {
      for (const point of ring) {
        const { x, y } = project(point);
        if (x < minX) minX = x;
        if (x > maxX) maxX = x;
        if (y < minY) minY = y;
        if (y > maxY) maxY = y;
      }
    }
  }
  if (!Number.isFinite(minX)) throw new RangeError("No coordinates found in the geometries.");
  return { minX, minY, maxX, maxY };
}

function ringsOf(geometry: { type: string; coordinates: unknown }): Ring[] {
  if (geometry.type === "Polygon") return geometry.coordinates as PolygonCoords;
  if (geometry.type === "MultiPolygon") {
    return (geometry.coordinates as MultiPolygonCoords).flat();
  }
  throw new TypeError(
    `Unsupported geometry type '${geometry.type}'. The pipeline publishes Polygon and MultiPolygon only.`,
  );
}

/**
 * An SVG path for one borough, in viewBox units.
 *
 * Coordinates are rounded to `dp` decimals. At the default 1 dp in a 1000-unit
 * box that is a tenth of a unit — sub-pixel at every zoom the UI offers — and it
 * roughly halves the payload the server sends to the browser. Rounding is done
 * here, once, rather than in the renderer, so the client never carries the
 * unrounded numbers at all.
 *
 * Rings are closed with `Z`; a MultiPolygon's islands and its holes are separate
 * subpaths of one path, which `fill-rule: evenodd` then renders correctly.
 */
export function pathFor(
  geometry: { type: string; coordinates: unknown },
  fit: Fit,
  dp = 1,
): string {
  const factor = 10 ** dp;
  const round = (n: number) => Math.round(n * factor) / factor;

  const parts: string[] = [];
  for (const ring of ringsOf(geometry)) {
    if (ring.length < 3) continue;
    const commands: string[] = [];
    let prev = "";
    for (let i = 0; i < ring.length; i++) {
      const { x, y } = applyFit(project(ring[i]), fit);
      const token = `${round(x)} ${round(y)}`;
      // Generalised boundaries repeat vertices; after rounding, neighbours can
      // collapse onto each other. Dropping the duplicates shortens the payload
      // and changes nothing about the rendered shape.
      if (token === prev) continue;
      commands.push(`${i === 0 ? "M" : "L"}${token}`);
      prev = token;
    }
    if (commands.length >= 3) parts.push(`${commands.join("")}Z`);
  }
  if (!parts.length) throw new RangeError("Geometry produced no drawable rings.");
  return parts.join("");
}

/** Centroid of the largest ring, for placing a label or a hover marker. */
export function anchorFor(
  geometry: { type: string; coordinates: unknown },
  fit: Fit,
): { x: number; y: number } {
  let best: Ring | null = null;
  let bestArea = -1;
  for (const ring of ringsOf(geometry)) {
    const area = Math.abs(signedArea(ring));
    if (area > bestArea) {
      bestArea = area;
      best = ring;
    }
  }
  if (!best) throw new RangeError("Geometry has no rings to anchor to.");

  // Area-weighted polygon centroid, not the mean of the vertices: generalised
  // boundaries have vertices bunched along complex edges, and a vertex mean is
  // pulled towards them.
  let cx = 0;
  let cy = 0;
  let a2 = 0;
  const pts = best.map((p) => applyFit(project(p), fit));
  for (let i = 0; i < pts.length - 1; i++) {
    const cross = pts[i].x * pts[i + 1].y - pts[i + 1].x * pts[i].y;
    a2 += cross;
    cx += (pts[i].x + pts[i + 1].x) * cross;
    cy += (pts[i].y + pts[i + 1].y) * cross;
  }
  if (a2 === 0) return pts[0];
  return { x: cx / (3 * a2), y: cy / (3 * a2) };
}

function signedArea(ring: Ring): number {
  let sum = 0;
  for (let i = 0; i < ring.length - 1; i++) {
    sum += ring[i][0] * ring[i + 1][1] - ring[i + 1][0] * ring[i][1];
  }
  return sum / 2;
}
