import { describe, expect, it } from "vitest";

import {
  anchorFor,
  applyFit,
  extentOf,
  fitExtent,
  pathFor,
  project,
} from "@/lib/projection";

/**
 * The projection is hand-written, so it is checked against the closed form
 * rather than against itself. The expected values below were computed
 * independently (Python's math module) from
 *   y = degrees(ln(tan(pi/4 + radians(lat)/2)))
 * which is the Web Mercator definition. A test that recomputed them with the
 * same code under test would pass no matter what that code did.
 */
describe("project", () => {
  it("matches the closed form at known latitudes", () => {
    expect(project([0, 0]).y).toBeCloseTo(0, 12);
    expect(project([0, 45]).y).toBeCloseTo(50.49898671052621, 10);
    expect(project([0, 51.5]).y).toBeCloseTo(60.278923622476746, 10);
    expect(project([0, -51.5]).y).toBeCloseTo(-60.27892362247676, 10);
  });

  it("leaves longitude untouched : Mercator only distorts north–south", () => {
    expect(project([-0.5103, 51.5]).x).toBe(-0.5103);
    expect(project([0.334, 51.28]).x).toBe(0.334);
  });

  it("is monotonic in latitude across London's extent", () => {
    let previous = -Infinity;
    for (let lat = 51.28; lat <= 51.7; lat += 0.01) {
      const { y } = project([0, lat]);
      expect(y).toBeGreaterThan(previous);
      previous = y;
    }
  });

  it("rejects coordinates outside the projection's domain rather than returning Infinity", () => {
    expect(() => project([0, 90])).toThrow(/outside the Mercator domain/);
    expect(() => project([0, -90])).toThrow(/outside the Mercator domain/);
    expect(() => project([Number.NaN, 51.5])).toThrow(/Non-finite/);
  });
});

describe("fitExtent", () => {
  const extent = { minX: -1, minY: -1, maxX: 1, maxY: 1 };

  it("fills the box and centres a square extent", () => {
    const fit = fitExtent(extent, 100, 100);
    expect(applyFit({ x: -1, y: 1 }, fit)).toEqual({ x: 0, y: 0 });
    expect(applyFit({ x: 1, y: -1 }, fit)).toEqual({ x: 100, y: 100 });
  });

  it("flips the y axis, because SVG counts downward and latitude counts up", () => {
    const fit = fitExtent(extent, 100, 100);
    const north = applyFit({ x: 0, y: 1 }, fit);
    const south = applyFit({ x: 0, y: -1 }, fit);
    expect(north.y).toBeLessThan(south.y);
  });

  it("preserves aspect ratio and centres in the slack axis", () => {
    // A square extent in a 200x100 box: scaled to 100 tall, centred horizontally.
    const fit = fitExtent(extent, 200, 100);
    expect(applyFit({ x: -1, y: 1 }, fit).x).toBeCloseTo(50, 9);
    expect(applyFit({ x: 1, y: 1 }, fit).x).toBeCloseTo(150, 9);
    expect(applyFit({ x: -1, y: 1 }, fit).y).toBeCloseTo(0, 9);
  });

  it("honours padding on both sides", () => {
    const fit = fitExtent(extent, 100, 100, 10);
    expect(applyFit({ x: -1, y: 1 }, fit)).toEqual({ x: 10, y: 10 });
    expect(applyFit({ x: 1, y: -1 }, fit)).toEqual({ x: 90, y: 90 });
  });

  it("refuses an extent with no area instead of dividing by zero", () => {
    expect(() => fitExtent({ minX: 0, minY: 0, maxX: 0, maxY: 1 }, 10, 10)).toThrow(/no area/);
    expect(() => fitExtent(extent, 10, 10, 6)).toThrow(/no room/);
  });
});

const SQUARE = {
  type: "Polygon",
  coordinates: [
    [
      [0, 0],
      [1, 0],
      [1, 1],
      [0, 1],
      [0, 0],
    ],
  ],
};

describe("pathFor", () => {
  const fit = fitExtent(extentOf([SQUARE]), 100, 100);

  it("closes every ring", () => {
    expect(pathFor(SQUARE, fit)).toMatch(/Z$/);
  });

  it("emits one subpath per ring, so islands and holes stay separate", () => {
    const withHole = {
      type: "Polygon",
      coordinates: [
        SQUARE.coordinates[0],
        [
          [0.25, 0.25],
          [0.75, 0.25],
          [0.75, 0.75],
          [0.25, 0.75],
          [0.25, 0.25],
        ],
      ],
    };
    expect(pathFor(withHole, fit).match(/M/g)).toHaveLength(2);
  });

  it("flattens a MultiPolygon's parts into subpaths of one path", () => {
    const multi = {
      type: "MultiPolygon",
      coordinates: [SQUARE.coordinates, SQUARE.coordinates],
    };
    expect(pathFor(multi, fit).match(/Z/g)).toHaveLength(2);
  });

  it("rounds to the requested precision", () => {
    const path = pathFor(SQUARE, fit, 1);
    for (const n of path.match(/-?\d+(\.\d+)?/g) ?? []) {
      expect(n).toMatch(/^-?\d+(\.\d)?$/);
    }
  });

  it("drops vertices that collapse onto each other after rounding", () => {
    // Two points 1e-6 apart round to the same viewBox coordinate.
    const nearly = {
      type: "Polygon",
      coordinates: [
        [
          [0, 0],
          [0.000001, 0],
          [1, 0],
          [1, 1],
          [0, 0],
        ],
      ],
    };
    const commands = pathFor(nearly, fit, 1).match(/[ML]/g) ?? [];
    expect(commands).toHaveLength(4);
  });

  it("rejects geometry types the pipeline does not publish", () => {
    expect(() => pathFor({ type: "Point", coordinates: [0, 0] }, fit)).toThrow(
      /Unsupported geometry type/,
    );
  });
});

describe("anchorFor", () => {
  it("returns the area-weighted centroid, not the mean of the vertices", () => {
    // Vertices bunched along one edge: a vertex mean is pulled towards them,
    // an area centroid is not. Generalised boundaries look exactly like this.
    const lopsided = {
      type: "Polygon",
      coordinates: [
        [
          [0, 0],
          [0.1, 0],
          [0.2, 0],
          [0.3, 0],
          [1, 0],
          [1, 1],
          [0, 1],
          [0, 0],
        ],
      ],
    };
    const fit = fitExtent(extentOf([lopsided]), 100, 100);
    const anchor = anchorFor(lopsided, fit);
    expect(anchor.x).toBeCloseTo(50, 6);
    expect(anchor.y).toBeCloseTo(50, 6);
  });

  it("anchors a MultiPolygon in its largest part, not between the parts", () => {
    const mainlandAndIsland = {
      type: "MultiPolygon",
      coordinates: [
        [
          [
            [0, 0],
            [1, 0],
            [1, 1],
            [0, 1],
            [0, 0],
          ],
        ],
        [
          [
            [9, 9],
            [9.05, 9],
            [9.05, 9.05],
            [9, 9.05],
            [9, 9],
          ],
        ],
      ],
    };
    const fit = fitExtent(extentOf([mainlandAndIsland]), 100, 100);
    const anchor = anchorFor(mainlandAndIsland, fit);
    // The big square occupies the lower-left of the frame; the anchor must be
    // inside it rather than out in the empty middle.
    expect(anchor.x).toBeLessThan(20);
    expect(anchor.y).toBeGreaterThan(80);
  });
});
