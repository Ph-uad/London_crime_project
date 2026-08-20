/**
 * Route tests for the data API (plan issues 2.1, 2.2).
 *
 * These exist for the same reason pipeline/tests/smoke.R does: the R side learned
 * that code which type-checks and builds is not thereby code that works. Every
 * test below asserts something that can actually be got wrong — a filter that
 * silently returns everything, a 400 that never fires, an array that collapses to
 * a scalar, a direction flag that flips.
 */
import { describe, expect, it } from "vitest";

import { GET as geo } from "@/app/api/geo/route";
import { GET as meta } from "@/app/api/meta/route";
import { GET as metrics } from "@/app/api/metrics/route";
import { allYears, boroughs, coverage, metricIds } from "@/lib/data";
import type {
  BoroughFeatureCollection,
  CoverageMatrix,
  MetricsResponse,
} from "@/lib/types";

const call = async <T>(handler: (r: Request) => Promise<Response>, query = "") => {
  const res = await handler(new Request(`http://test/api${query}`));
  return { res, body: (await res.json()) as T };
};

const AN_ANNUAL_METRIC = "crime_rate_per_1000";

describe("GET /api/metrics — the full dataset", () => {
  it("returns every observation when unfiltered", async () => {
    const { res, body } = await call<MetricsResponse>(metrics);
    expect(res.status).toBe(200);
    expect(body.count).toBe(body.observations.length);
    expect(body.count).toBeGreaterThan(0);
    expect(body.boroughs).toHaveLength(33);
  });

  it("caches at the edge but not in the browser", async () => {
    const { res } = await call(metrics);
    const cc = res.headers.get("cache-control") ?? "";
    expect(cc).toContain("s-maxage=31536000");
    expect(cc).toContain("max-age=0");
    expect(cc).toContain("stale-while-revalidate");
  });

  it("carries coverage for exactly the metrics it returned", async () => {
    const { body } = await call<MetricsResponse>(metrics, `?metric=${AN_ANNUAL_METRIC}`);
    expect(Object.keys(body.coverage)).toEqual([AN_ANNUAL_METRIC]);
  });
});

describe("GET /api/metrics — filtering", () => {
  it("filters by metric", async () => {
    const { body } = await call<MetricsResponse>(metrics, `?metric=${AN_ANNUAL_METRIC}`);
    expect(body.observations.every((o) => o.metric === AN_ANNUAL_METRIC)).toBe(true);
    expect(body.observations.length).toBeGreaterThan(0);
  });

  it("filters by year", async () => {
    const year = allYears[0];
    const { body } = await call<MetricsResponse>(metrics, `?year=${year}`);
    expect(body.observations.every((o) => o.year === year)).toBe(true);
  });

  it("filters by borough GSS code", async () => {
    const gss = boroughs[0].gss;
    const { body } = await call<MetricsResponse>(metrics, `?borough=${gss}`);
    expect(body.observations.every((o) => o.borough_gss === gss)).toBe(true);
  });

  it("AND-s filters together rather than widening", async () => {
    const gss = boroughs[0].gss;
    const year = allYears[0];
    const { body } = await call<MetricsResponse>(
      metrics,
      `?metric=${AN_ANNUAL_METRIC}&year=${year}&borough=${gss}`,
    );
    expect(body.observations.length).toBeLessThanOrEqual(1);
    for (const o of body.observations) {
      expect(o).toMatchObject({ metric: AN_ANNUAL_METRIC, year, borough_gss: gss });
    }
  });

  it("accepts comma-separated lists", async () => {
    const two = metricIds.slice(0, 2);
    const { body } = await call<MetricsResponse>(metrics, `?metric=${two.join(",")}`);
    expect(new Set(body.observations.map((o) => o.metric))).toEqual(new Set(two));
  });

  it("returns an empty result, not an error, for a valid but unmatched combination", async () => {
    // Well-being has no City of London data by design.
    const wellbeing = metricIds.find((m) => m.startsWith("wellbeing_"));
    if (!wellbeing) return;
    const { res, body } = await call<MetricsResponse>(
      metrics,
      `?metric=${wellbeing}&borough=E09000001`,
    );
    expect(res.status).toBe(200);
    expect(body.count).toBe(0);
  });
});

describe("GET /api/metrics — invalid parameters return 400", () => {
  it("rejects an unknown metric and says where to look", async () => {
    const { res, body } = await call<{ error: string; detail: string }>(
      metrics,
      "?metric=not_a_metric",
    );
    expect(res.status).toBe(400);
    expect(body.error).toMatch(/unknown metric/i);
    expect(body.detail).toContain("not_a_metric");
  });

  it("rejects a non-numeric year", async () => {
    const { res, body } = await call<{ error: string }>(metrics, "?year=last");
    expect(res.status).toBe(400);
    expect(body.error).toMatch(/invalid year/i);
  });

  it("rejects a year outside the dataset", async () => {
    const { res } = await call(metrics, "?year=1850");
    expect(res.status).toBe(400);
  });

  it("rejects an unknown borough code and explains codes-not-names", async () => {
    const { res, body } = await call<{ detail: string }>(metrics, "?borough=Camden");
    expect(res.status).toBe(400);
    expect(body.detail).toMatch(/GSS code/i);
  });

  // The one that matters most: a typo'd parameter must not quietly return
  // everything and look like it worked.
  it("rejects an unknown parameter rather than ignoring it", async () => {
    const { res, body } = await call<{ error: string }>(metrics, "?metrics=crime_count");
    expect(res.status).toBe(400);
    expect(body.error).toMatch(/unknown query parameter/i);
  });

  it("rejects an empty parameter", async () => {
    const { res } = await call(metrics, "?metric=");
    expect(res.status).toBe(400);
  });

  it("never caches an error", async () => {
    const { res } = await call(metrics, "?metric=nope");
    expect(res.headers.get("cache-control")).toBe("no-store");
  });
});

describe("GET /api/meta — the coverage contract", () => {
  it("returns the whole matrix", async () => {
    const { res, body } = await call<CoverageMatrix>(meta);
    expect(res.status).toBe(200);
    expect(Object.keys(body.metrics).sort()).toEqual([...metricIds]);
    expect(body.boroughs).toHaveLength(33);
    expect(body.window.analysis_start).toBeLessThan(body.window.trend_end);
  });

  it("declares direction, scale, cadence and year_rule for every metric", async () => {
    const { body } = await call<CoverageMatrix>(meta);
    for (const [id, m] of Object.entries(body.metrics)) {
      expect(["higher_is_better", "higher_is_worse", "neutral"], id).toContain(m.direction);
      expect(["annual", "snapshot"], id).toContain(m.cadence);
      expect(["calendar", "financial_start", "rolling_end", "snapshot"], id).toContain(
        m.year_rule,
      );
      expect(m.scale, id).toBeTruthy();
      expect(m.label, id).toBeTruthy();
    }
  });

  // A frontend that assumes "up is good" gets anxiety and crime backwards.
  it("marks anxiety and crime as higher_is_worse", async () => {
    const { body } = await call<CoverageMatrix>(meta);
    for (const id of ["wellbeing_anxiety", "crime_rate_per_1000", "crime_count"]) {
      if (body.metrics[id]) expect(body.metrics[id].direction, id).toBe("higher_is_worse");
    }
  });

  it("marks IMD metrics as snapshots, so they do not drive a continuous slider", async () => {
    const { body } = await call<CoverageMatrix>(meta);
    for (const [id, m] of Object.entries(body.metrics)) {
      if (id.startsWith("imd_")) expect(m.cadence, id).toBe("snapshot");
    }
  });

  // Serialisation trap: a one-element array must not arrive as a bare scalar.
  it("keeps years and partial_years as arrays at every length", async () => {
    const { body } = await call<CoverageMatrix>(meta);
    for (const [id, m] of Object.entries(body.metrics)) {
      expect(Array.isArray(m.years), id).toBe(true);
      expect(Array.isArray(m.partial_years), id).toBe(true);
      expect(Array.isArray(m.boroughs_missing), id).toBe(true);
    }
  });

  it("reports the boroughs a metric does not cover", async () => {
    const { body } = await call<CoverageMatrix>(meta);
    for (const m of Object.values(body.metrics)) {
      expect(m.boroughs_covered + m.boroughs_missing.length).toBe(33);
    }
  });

  it("never lets the IMD crime domain through", async () => {
    const { body } = await call<CoverageMatrix>(meta);
    expect(Object.keys(body.metrics).filter((m) => m.startsWith("imd_crime"))).toEqual([]);
  });

  it("rejects query parameters", async () => {
    const { res } = await call(meta, "?metric=crime_count");
    expect(res.status).toBe(400);
  });
});

describe("GET /api/geo — borough boundaries", () => {
  it("returns 33 features with the GeoJSON media type", async () => {
    const { res, body } = await call<BoroughFeatureCollection>(geo);
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("application/geo+json");
    expect(body.type).toBe("FeatureCollection");
    expect(body.features).toHaveLength(33);
  });

  it("carries GSS codes matching the metrics data", async () => {
    const { body } = await call<BoroughFeatureCollection>(geo);
    const inGeo = new Set(body.features.map((f) => f.properties.borough_gss));
    const inData = new Set(coverage.boroughs.map((b) => b.gss));
    expect(inGeo).toEqual(inData);
  });

  // British National Grid eastings here would render the map in the North Sea.
  it("is in WGS84 degrees, not British National Grid", async () => {
    const { body } = await call<BoroughFeatureCollection>(geo);
    const coords = JSON.stringify(body.features.map((f) => f.geometry.coordinates));
    const nums = (coords.match(/-?\d+(\.\d+)?/g) ?? []).map(Number);
    expect(Math.min(...nums)).toBeGreaterThan(-2);
    expect(Math.max(...nums)).toBeLessThan(53);
  });

  it("rejects query parameters", async () => {
    const { res } = await call(geo, "?borough=E09000007");
    expect(res.status).toBe(400);
  });
});
