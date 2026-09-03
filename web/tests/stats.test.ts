import { describe, expect, it } from "vitest";

import { coverage } from "@/lib/data";
import {
  describeStrength,
  fitLine,
  longRunChange,
  ordinal,
  rankOf,
  summarise,
  worstEnd,
} from "@/lib/stats";
import type { MetricCoverage } from "@/lib/types";

describe("rankOf", () => {
  const values = [10, 20, 30, 40, 50];

  it("ranks the largest first for a higher_is_worse metric", () => {
    expect(rankOf(values, 4, "higher_is_worse")?.rank).toBe(1);
    expect(rankOf(values, 0, "higher_is_worse")?.rank).toBe(5);
  });

  it("ranks the smallest first for a higher_is_better metric, so rank 1 is always the worst", () => {
    expect(rankOf(values, 0, "higher_is_better")?.rank).toBe(1);
    expect(rankOf(values, 4, "higher_is_better")?.rank).toBe(5);
  });

  it("marks a neutral metric as having no worst end", () => {
    expect(rankOf(values, 4, "neutral")?.worstFirst).toBe(false);
    expect(rankOf(values, 4, "higher_is_worse")?.worstFirst).toBe(true);
  });

  it("uses the count of boroughs WITH a value as the denominator", () => {
    // The live case: well-being covers 32 of 33 boroughs, so a rank is n/32.
    // Reporting n/33 would make a claim about City of London that the source
    // does not make.
    const withHole = [null, 20, 30, 40, 50];
    expect(rankOf(withHole, 1, "higher_is_worse")?.of).toBe(4);
  });

  it("gives tied values the same rank and reports how many are tied", () => {
    const tied = [10, 20, 20, 20, 50];
    const r = rankOf(tied, 1, "higher_is_worse");
    expect(r?.rank).toBe(2);
    expect(r?.tied).toBe(3);
    // Competition ranking: the next distinct value takes rank 5, not 3.
    expect(rankOf(tied, 0, "higher_is_worse")?.rank).toBe(5);
  });

  it("returns null for a borough with no value rather than ranking it last", () => {
    expect(rankOf([null, 1, 2], 0, "higher_is_worse")).toBeNull();
  });
});

describe("ordinal", () => {
  it("handles the teens, which the naive rule gets wrong", () => {
    expect(["11th", "12th", "13th"]).toEqual([ordinal(11), ordinal(12), ordinal(13)]);
  });

  it("handles the ordinary cases", () => {
    expect([ordinal(1), ordinal(2), ordinal(3), ordinal(4), ordinal(21), ordinal(33)]).toEqual([
      "1st",
      "2nd",
      "3rd",
      "4th",
      "21st",
      "33rd",
    ]);
  });
});

describe("summarise", () => {
  it("ignores holes in both the mean and the count", () => {
    const s = summarise([10, null, 30]);
    expect(s?.n).toBe(2);
    expect(s?.mean).toBe(20);
  });

  it("reports which borough holds each extreme, by index", () => {
    const s = summarise([30, 10, 20]);
    expect(s?.min.index).toBe(1);
    expect(s?.max.index).toBe(0);
  });

  it("returns null when nothing has a value", () => {
    expect(summarise([null, null])).toBeNull();
  });
});

describe("worstEnd", () => {
  it("names the bad extreme per direction, and refuses to name one for neutral", () => {
    expect(worstEnd("higher_is_worse")).toBe("max");
    expect(worstEnd("higher_is_better")).toBe("min");
    expect(worstEnd("neutral")).toBeNull();
  });
});

describe("fitLine", () => {
  it("recovers a known line exactly", () => {
    const pairs = [0, 1, 2, 3, 4].map((x) => ({ index: x, x, y: 3 * x + 2 }));
    const fit = fitLine(pairs);
    expect(fit?.slope).toBeCloseTo(3, 12);
    expect(fit?.intercept).toBeCloseTo(2, 12);
    expect(fit?.r).toBeCloseTo(1, 12);
  });

  it("gives r = −1 for a perfect inverse relationship", () => {
    const pairs = [0, 1, 2, 3, 4].map((x) => ({ index: x, x, y: -2 * x }));
    expect(fitLine(pairs)?.r).toBeCloseTo(-1, 12);
  });

  it("matches an independently computed r on a known dataset", () => {
    // Anscombe's first quartet: r = 0.816 to three decimals, a value published
    // in every statistics text, so this checks the formula rather than itself.
    const x = [10, 8, 13, 9, 11, 14, 6, 4, 12, 7, 5];
    const y = [8.04, 6.95, 7.58, 8.81, 8.33, 9.96, 7.24, 4.26, 10.84, 4.82, 5.68];
    const fit = fitLine(x.map((xi, i) => ({ index: i, x: xi, y: y[i] })));
    expect(fit?.r).toBeCloseTo(0.816, 3);
    expect(fit?.slope).toBeCloseTo(0.5, 2);
    expect(fit?.intercept).toBeCloseTo(3.0, 1);
  });

  it("returns null rather than NaN when x has no variance", () => {
    // Live case: imd_employment_score has one distinct value across all 33
    // boroughs, so a scatter against it has an undefined slope.
    const flat = [1, 2, 3].map((y, i) => ({ index: i, x: 0.1, y }));
    expect(fitLine(flat)).toBeNull();
  });

  it("returns null below three points", () => {
    expect(fitLine([{ index: 0, x: 1, y: 1 }, { index: 1, x: 2, y: 2 }])).toBeNull();
  });
});

describe("describeStrength", () => {
  it("is symmetric in sign : a strong negative association is still strong", () => {
    expect(describeStrength(-0.8)).toBe(describeStrength(0.8));
  });

  it("labels the conventional bands", () => {
    expect(describeStrength(0.75)).toBe("strong");
    expect(describeStrength(0.55)).toBe("moderate");
    expect(describeStrength(0.35)).toBe("weak");
    expect(describeStrength(0.1)).toBe("very weak");
  });
});

function metric(overrides: Partial<MetricCoverage> = {}): MetricCoverage {
  return {
    label: "Test",
    cadence: "annual",
    direction: "higher_is_worse",
    scale: "count",
    unit: "u",
    year_rule: "calendar",
    years: [2011, 2012, 2013],
    partial_years: [],
    boroughs_covered: 33,
    boroughs_missing: [],
    observations: 99,
    source: "test",
    ...overrides,
  };
}

describe("longRunChange", () => {
  const values: Record<number, number> = { 2011: 100, 2012: 110, 2013: 90, 2026: 20 };
  const valueOf = (y: number) => values[y] ?? null;

  it("spans the first and last usable years of the metric's own series", () => {
    const change = longRunChange(metric(), valueOf);
    expect(change?.fromYear).toBe(2011);
    expect(change?.toYear).toBe(2013);
    expect(change?.delta).toBe(-10);
  });

  it("SKIPS a partial year at the end, which would otherwise report a fake collapse", () => {
    // crime_count runs to 2026, but 2026 is four months of data. Using it as
    // the endpoint reports a 80% fall in crime that is really a fall in
    // coverage.
    const withPartial = metric({ years: [2011, 2012, 2013, 2026], partial_years: [2026] });
    const change = longRunChange(withPartial, valueOf);
    expect(change?.toYear).toBe(2013);
    expect(change?.delta).toBe(-10);
  });

  it("reads improvement from direction, not from the sign", () => {
    // Falling crime is an improvement.
    expect(longRunChange(metric({ direction: "higher_is_worse" }), valueOf)?.improved).toBe(true);
    // Falling life expectancy is not.
    expect(longRunChange(metric({ direction: "higher_is_better" }), valueOf)?.improved).toBe(false);
    // A taxpayer count has no better direction to move in.
    expect(longRunChange(metric({ direction: "neutral" }), valueOf)?.improved).toBeNull();
  });

  it("REFUSES a trend across snapshot metrics", () => {
    // IMD 2015 and IMD 2019 are separate exercises. The pipeline already drops
    // their ranks as non-comparable; presenting a change between their scores
    // as a long-run trend would put a number on something the source does not
    // support. Every IMD metric in the dataset must return null.
    const imd = metric({ cadence: "snapshot", years: [2015, 2019], observations: 66 });
    expect(longRunChange(imd, (y) => ({ 2015: 30, 2019: 25 })[y] ?? null)).toBeNull();

    for (const [id, cov] of Object.entries(coverage.metrics)) {
      if (cov.cadence !== "snapshot") continue;
      expect(longRunChange(cov, () => 1), id).toBeNull();
    }
  });

  it("still computes a trend for annual metrics with only two years", () => {
    const short = metric({ years: [2011, 2012] });
    expect(longRunChange(short, valueOf)?.delta).toBe(10);
  });

  it("returns null when only one year has a value", () => {
    expect(longRunChange(metric(), (y) => (y === 2012 ? 5 : null))).toBeNull();
  });

  it("handles the real crime_count coverage without picking up 2026", () => {
    const crime = coverage.metrics.crime_count;
    expect(crime.partial_years).toContain(2026);
    const change = longRunChange(crime, (y) => (crime.years.includes(y) ? y : null));
    expect(change?.toYear).not.toBe(2026);
    expect(change?.toYear).toBe(2025);
  });
});
