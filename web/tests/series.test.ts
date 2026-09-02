import { describe, expect, it } from "vitest";

import { coverage, boroughs, observations } from "@/lib/data";
import {
  absenceReason,
  buildSeries,
  nearestCompleteYear,
  nearestYear,
  rowFor,
  unionYears,
  valueAt,
} from "@/lib/series";
import type { BoroughRef, MetricCoverage, Observation } from "@/lib/types";

const REFS: BoroughRef[] = [
  { gss: "E09000001", name: "City of London" },
  { gss: "E09000007", name: "Camden" },
  { gss: "E09000022", name: "Lambeth" },
];

function obs(gss: string, year: number, metric: string, value: number): Observation {
  return { borough_gss: gss, year, metric, value };
}

describe("buildSeries", () => {
  it("indexes by metric, year and borough position", () => {
    const series = buildSeries(
      [obs("E09000007", 2019, "crime_rate_per_1000", 120), obs("E09000022", 2019, "crime_rate_per_1000", 95)],
      REFS,
    );
    expect(series.crime_rate_per_1000[2019]).toEqual([null, 120, 95]);
  });

  it("leaves an explicit null for a borough with no observation, not a short row", () => {
    const series = buildSeries([obs("E09000007", 2019, "wellbeing_anxiety", 3.1)], REFS);
    const row = series.wellbeing_anxiety[2019];
    expect(row).toHaveLength(REFS.length);
    expect(row[0]).toBeNull();
    // The distinction that matters: absent is not zero.
    expect(row[0]).not.toBe(0);
  });

  it("throws on an observation for a borough outside the reference list", () => {
    expect(() => buildSeries([obs("E06000001", 2019, "crime_count", 1)], REFS)).toThrow(
      /unknown borough 'E06000001'/,
    );
  });

  it("indexes the real export without loss", () => {
    const series = buildSeries(observations, boroughs);
    const counted = Object.values(series)
      .flatMap((byYear) => Object.values(byYear))
      .flat()
      .filter((v) => v !== null).length;
    expect(counted).toBe(observations.length);
  });
});

describe("rowFor and valueAt", () => {
  const series = buildSeries([obs("E09000007", 2019, "income_median", 31000)], REFS);

  it("returns an all-null row for a year the metric does not have", () => {
    expect(rowFor(series, "income_median", 1066, REFS.length)).toEqual([null, null, null]);
  });

  it("returns an all-null row for a metric that does not exist", () => {
    expect(rowFor(series, "not_a_metric", 2019, REFS.length)).toEqual([null, null, null]);
  });

  it("reads a single value by position", () => {
    expect(valueAt(series, "income_median", 2019, 1)).toBe(31000);
    expect(valueAt(series, "income_median", 2019, 0)).toBeNull();
  });
});

const ANNUAL: MetricCoverage = {
  label: "Test",
  cadence: "annual",
  direction: "higher_is_better",
  scale: "rate",
  unit: "u",
  year_rule: "calendar",
  years: [2011, 2012, 2015, 2016],
  partial_years: [2016],
  boroughs_covered: 3,
  boroughs_missing: [],
  observations: 12,
  source: "test",
};

describe("nearestYear", () => {
  it("returns the year itself when it exists", () => {
    expect(nearestYear(ANNUAL, 2012)).toBe(2012);
  });

  it("finds the closest year across a gap", () => {
    expect(nearestYear(ANNUAL, 2014)).toBe(2015);
    expect(nearestYear(ANNUAL, 2020)).toBe(2016);
  });

  it("breaks a tie towards the earlier year, so a pairing never drifts forward", () => {
    // 2013 is one from 2012 and two from 2015; 2013.5 would tie. Use an exact
    // tie: target 2013.5 is not an integer year, so construct one directly.
    expect(nearestYear({ ...ANNUAL, years: [2010, 2014] }, 2012)).toBe(2010);
  });

  it("returns null when the metric has no years at all", () => {
    expect(nearestYear({ ...ANNUAL, years: [] }, 2019)).toBeNull();
  });
});

describe("nearestCompleteYear", () => {
  it("skips a partial year even when it is closest", () => {
    expect(nearestCompleteYear(ANNUAL, 2016)).toBe(2015);
  });

  it("returns null when every year is partial", () => {
    expect(nearestCompleteYear({ ...ANNUAL, partial_years: ANNUAL.years }, 2015)).toBeNull();
  });
});

describe("absenceReason", () => {
  const wellbeing = coverage.metrics.wellbeing_anxiety;
  const city = { gss: "E09000001", name: "City of London" };
  const camden = { gss: "E09000007", name: "Camden" };

  it("distinguishes a borough the source does not cover", () => {
    const reason = absenceReason(wellbeing, "Anxiety", 2019, city);
    expect(reason.kind).toBe("not_covered");
    expect(reason.text).toContain("City of London");
    expect(reason.text).toContain("population is too small");
  });

  it("distinguishes a year outside the series, and names the real range", () => {
    const reason = absenceReason(wellbeing, "Anxiety", 2024, camden);
    expect(reason.kind).toBe("outside_series");
    // Well-being stops at 2022 while crime runs to 2024 — the live case.
    expect(reason.text).toContain("2011–2022");
  });

  it("says a snapshot metric exists only at its snapshot years", () => {
    const imd = coverage.metrics.imd_income_score;
    const reason = absenceReason(imd, "IMD income", 2018, camden);
    expect(reason.kind).toBe("outside_series");
    expect(reason.text).toContain("2015 and 2019");
  });

  it("distinguishes a hole inside the series from either of the above", () => {
    const reason = absenceReason(wellbeing, "Anxiety", 2019, camden);
    expect(reason.kind).toBe("no_observation");
  });
});

describe("unionYears", () => {
  it("is the union across metrics, not any one metric's range", () => {
    const years = unionYears(coverage.metrics, ["wellbeing_anxiety", "crime_rate_per_1000"]);
    expect(Math.min(...years)).toBe(2011);
    expect(Math.max(...years)).toBe(2024);
  });

  it("ignores metrics that do not exist rather than throwing", () => {
    expect(unionYears(coverage.metrics, ["nope"])).toEqual([]);
  });
});
