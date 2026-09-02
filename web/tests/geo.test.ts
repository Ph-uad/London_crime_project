import { describe, expect, it } from "vitest";

import { boroughs } from "@/lib/data";
import { boroughGeoJson, boroughShapes } from "@/lib/geo";

/**
 * These run against the real pipeline output, so they double as a contract check
 * on `pipeline/03_borough_boundaries.R`.
 */
describe("boroughShapes", () => {
  const shapes = boroughShapes(boroughs);

  it("produces one path per borough, index-aligned with the borough list", () => {
    expect(shapes.shapes).toHaveLength(boroughs.length);
    expect(shapes.anchors).toHaveLength(boroughs.length);
    expect(boroughs).toHaveLength(33);
  });

  it("joins on GSS code, not array position", () => {
    // The pipeline currently writes london.geojson in the same order as
    // boroughs.json, so a positional join would pass by luck today and draw
    // every borough with a neighbour's outline the moment either file is
    // re-sorted — which looks entirely plausible on a map of a city you do not
    // know well. Reordering the request is what distinguishes the two.
    const reversed = [...boroughs].reverse();
    const byReversed = boroughShapes(reversed);

    for (let i = 0; i < reversed.length; i++) {
      const forwardIndex = boroughs.findIndex((b) => b.gss === reversed[i].gss);
      expect(byReversed.shapes[i], reversed[i].name).toBe(shapes.shapes[forwardIndex]);
    }
    // And the order really did change, so the check above is not vacuous.
    expect(byReversed.shapes[0]).not.toBe(shapes.shapes[0]);
  });

  it("emits closed, non-empty paths in viewBox units", () => {
    for (let i = 0; i < shapes.shapes.length; i++) {
      const path = shapes.shapes[i];
      expect(path.startsWith("M"), boroughs[i].name).toBe(true);
      expect(path.endsWith("Z"), boroughs[i].name).toBe(true);
      expect(path.length).toBeGreaterThan(20);
    }
  });

  it("keeps every coordinate inside the viewBox", () => {
    const { width, height } = shapes.viewBox;
    for (let i = 0; i < shapes.shapes.length; i++) {
      const numbers = (shapes.shapes[i].match(/-?\d+(\.\d+)?/g) ?? []).map(Number);
      for (let j = 0; j < numbers.length; j += 2) {
        expect(numbers[j], `${boroughs[i].name} x`).toBeGreaterThanOrEqual(0);
        expect(numbers[j], `${boroughs[i].name} x`).toBeLessThanOrEqual(width);
        expect(numbers[j + 1], `${boroughs[i].name} y`).toBeGreaterThanOrEqual(0);
        expect(numbers[j + 1], `${boroughs[i].name} y`).toBeLessThanOrEqual(height);
      }
    }
  });

  it("puts every anchor inside the viewBox", () => {
    for (let i = 0; i < shapes.anchors.length; i++) {
      const a = shapes.anchors[i];
      expect(a.x, boroughs[i].name).toBeGreaterThanOrEqual(0);
      expect(a.x, boroughs[i].name).toBeLessThanOrEqual(shapes.viewBox.width);
      expect(a.y, boroughs[i].name).toBeGreaterThanOrEqual(0);
      expect(a.y, boroughs[i].name).toBeLessThanOrEqual(shapes.viewBox.height);
    }
  });

  it("places recognisable boroughs where London actually puts them", () => {
    // A projection bug that flips or transposes an axis still produces 33 valid
    // paths inside the box. Geography is the check that catches it.
    const at = (gss: string) => shapes.anchors[boroughs.findIndex((b) => b.gss === gss)];
    const havering = at("E09000016"); // far east
    const hillingdon = at("E09000017"); // far west
    const enfield = at("E09000010"); // far north
    const croydon = at("E09000008"); // far south

    expect(havering.x).toBeGreaterThan(hillingdon.x);
    expect(enfield.y).toBeLessThan(croydon.y); // SVG y grows downward
  });

  it("is meaningfully smaller than shipping the raw coordinates", () => {
    const raw = JSON.stringify(boroughGeoJson()).length;
    const projected = JSON.stringify(shapes).length;
    expect(projected).toBeLessThan(raw);
  });

  it("fails loudly when a borough has no geometry", () => {
    expect(() => boroughShapes([...boroughs, { gss: "E09000099", name: "Nowhere" }])).toThrow(
      /No boundary geometry for Nowhere/,
    );
  });

  it("caches on the borough list, and rebuilds when it changes", () => {
    const again = boroughShapes(boroughs);
    expect(again).toBe(shapes);
    const subset = boroughShapes(boroughs.slice(0, 5));
    expect(subset).not.toBe(shapes);
    expect(subset.shapes).toHaveLength(5);
  });
});

describe("the GeoJSON contract", () => {
  it("carries 33 features with the properties the map joins on", () => {
    const fc = boroughGeoJson();
    expect(fc.type).toBe("FeatureCollection");
    expect(fc.features).toHaveLength(33);
    for (const f of fc.features) {
      expect(f.properties.borough_gss).toMatch(/^E09\d{6}$/);
      expect(f.properties.borough_name.length).toBeGreaterThan(0);
      expect(["Polygon", "MultiPolygon"]).toContain(f.geometry.type);
    }
  });

  it("agrees with boroughs.json about which boroughs exist", () => {
    const geo = new Set(boroughGeoJson().features.map((f) => f.properties.borough_gss));
    const data = new Set(boroughs.map((b) => b.gss));
    expect([...data].filter((c) => !geo.has(c))).toEqual([]);
    expect([...geo].filter((c) => !data.has(c))).toEqual([]);
  });
});
