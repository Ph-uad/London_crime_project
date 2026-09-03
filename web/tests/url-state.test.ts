import { describe, expect, it } from "vitest";

import { coverage } from "@/lib/data";
import {
  DEFAULT_COMPARE,
  DEFAULT_METRIC,
  defaultYear,
  parseState,
  snapYear,
  toSearchString,
} from "@/lib/url-state";

const parse = (query: string) => parseState(new URLSearchParams(query), coverage);

describe("defaults", () => {
  it("opens on the crime rate, the project's outcome variable", () => {
    const { state, rejected } = parse("");
    expect(state.metric).toBe(DEFAULT_METRIC);
    expect(state.compare).toBe(DEFAULT_COMPARE);
    expect(rejected).toEqual([]);
  });

  it("opens on the latest complete year of the selected metric", () => {
    // crime_rate_per_1000 runs to 2024 with no partial years.
    expect(parse("").state.year).toBe(2024);
  });

  it("never defaults to a partial year", () => {
    // crime_count runs to 2026, but 2026 is four months.
    expect(defaultYear(coverage.metrics.crime_count)).toBe(2025);
    expect(parse("metric=crime_count").state.year).toBe(2025);
  });
});

describe("parseState : valid input", () => {
  it("round-trips a fully specified state", () => {
    const { state, rejected } = parse(
      "metric=wellbeing_anxiety&year=2018&compare=income_median&exclude=E09000001&borough=E09000007",
    );
    expect(rejected).toEqual([]);
    expect(state).toEqual({
      metric: "wellbeing_anxiety",
      year: 2018,
      compare: "income_median",
      exclude: ["E09000001"],
      borough: "E09000007",
    });
  });

  it("ignores unknown parameters instead of refusing to render", () => {
    // Unlike the API, which rejects them: a page URL collects utm_source from
    // anything that links to it, and a tracking parameter must not break it.
    const { state, rejected } = parse("utm_source=twitter&fbclid=abc");
    expect(rejected).toEqual([]);
    expect(state.metric).toBe(DEFAULT_METRIC);
  });

  it("de-duplicates exclusions", () => {
    expect(parse("exclude=E09000001,E09000001").state.exclude).toEqual(["E09000001"]);
  });
});

describe("parseState : bad input falls back and says so", () => {
  it("falls back to the default metric and names the rejection", () => {
    const { state, rejected } = parse("metric=not_a_metric");
    expect(state.metric).toBe(DEFAULT_METRIC);
    expect(rejected[0]).toContain("not a metric");
  });

  it("snaps a year outside the metric's range onto its own range", () => {
    // The live case: well-being stops at 2022 while crime runs to 2024.
    const { state, rejected } = parse("metric=wellbeing_anxiety&year=2024");
    expect(state.year).toBe(2022);
    expect(rejected[0]).toContain("2011–2022");
    expect(rejected[0]).toContain("Showing 2022");
  });

  it("rejects a non-numeric year", () => {
    const { rejected } = parse("year=last");
    expect(rejected[0]).toContain("not a four-digit year");
  });

  it("drops a borough code that is not a London borough", () => {
    const { state, rejected } = parse("exclude=E06000001,E09000001");
    expect(state.exclude).toEqual(["E09000001"]);
    expect(rejected[0]).toContain("E06000001");
  });

  it("refuses an exclusion list that empties the map", () => {
    const all = coverage.boroughs.map((b) => b.gss).join(",");
    const { state, rejected } = parse(`exclude=${all}`);
    expect(state.exclude).toEqual([]);
    expect(rejected.at(-1)).toContain("Every borough was excluded");
  });

  it("does not open a detail panel for an unknown borough", () => {
    const { state, rejected } = parse("borough=E06000001");
    expect(state.borough).toBeNull();
    expect(rejected[0]).toContain("not a London borough");
  });
});

describe("snapYear", () => {
  it("leaves a year the metric has alone", () => {
    expect(snapYear(coverage.metrics.wellbeing_anxiety, 2015)).toBe(2015);
  });

  it("moves onto the nearest published year for a snapshot metric", () => {
    const imd = coverage.metrics.imd_income_score;
    expect(snapYear(imd, 2016)).toBe(2015);
    expect(snapYear(imd, 2018)).toBe(2019);
    expect(snapYear(imd, 2024)).toBe(2019);
  });
});

describe("toSearchString", () => {
  it("is empty for the default state, so the plain URL stays plain", () => {
    expect(toSearchString(parse("").state, coverage)).toBe("");
  });

  it("omits a year that is already the metric's default", () => {
    const state = { ...parse("").state, year: defaultYear(coverage.metrics[DEFAULT_METRIC]) };
    expect(toSearchString(state, coverage)).toBe("");
  });

  it("carries only what the reader changed", () => {
    const state = parse("metric=income_median&year=2015").state;
    const query = toSearchString(state, coverage);
    expect(query).toContain("metric=income_median");
    expect(query).toContain("year=2015");
    expect(query).not.toContain("compare=");
  });

  it("round-trips through parseState unchanged", () => {
    const original = parse(
      "metric=life_expectancy_birth_female&year=2019&compare=imd_income_score&exclude=E09000001&borough=E09000022",
    ).state;
    const reparsed = parse(toSearchString(original, coverage).replace(/^\?/, "")).state;
    expect(reparsed).toEqual(original);
  });

  it("round-trips every metric in the dataset", () => {
    for (const id of Object.keys(coverage.metrics)) {
      const original = parse(`metric=${id}`).state;
      const reparsed = parse(toSearchString(original, coverage).replace(/^\?/, "")).state;
      expect(reparsed, id).toEqual(original);
    }
  });
});
