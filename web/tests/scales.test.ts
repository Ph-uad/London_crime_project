import { describe, expect, it } from "vitest";

import { coverage } from "@/lib/data";
import { buildColourScale, legendRows, NO_DATA_FILL } from "@/lib/scales";
import type { MetricCoverage } from "@/lib/types";

function metric(overrides: Partial<MetricCoverage> = {}): MetricCoverage {
  return {
    label: "Test metric",
    cadence: "annual",
    direction: "higher_is_worse",
    scale: "rate",
    unit: "u",
    year_rule: "calendar",
    years: [2019],
    partial_years: [],
    boroughs_covered: 33,
    boroughs_missing: [],
    observations: 33,
    source: "test",
    ...overrides,
  };
}

/** 33 values, evenly spread, so quantile breaks are unambiguous. */
const SPREAD = Array.from({ length: 33 }, (_, i) => i + 1);

describe("buildColourScale : sequential", () => {
  it("uses seven classes when the data supports them", () => {
    const scale = buildColourScale(metric(), SPREAD);
    expect(scale.ramp).toBe("sequential");
    expect(scale.classes).toHaveLength(7);
  });

  it("colours the high end dark for a higher_is_worse metric", () => {
    const scale = buildColourScale(metric({ direction: "higher_is_worse" }), SPREAD);
    expect(scale.classes[0].fill).toBe("var(--seq-100)");
    expect(scale.classes[6].fill).toBe("var(--seq-700)");
    expect(scale.note).toContain("Darker means higher");
  });

  it("REVERSES the ramp for a higher_is_better metric, so dark still means worse", () => {
    // This is the whole reason `direction` is in the contract. Median income and
    // crime rate must not be read with opposite conventions on the same map.
    const scale = buildColourScale(metric({ direction: "higher_is_better" }), SPREAD);
    expect(scale.classes[0].fill).toBe("var(--seq-700)");
    expect(scale.classes[6].fill).toBe("var(--seq-100)");
    expect(scale.note).toContain("Darker means lower");
  });

  it("makes no better/worse claim for a neutral metric", () => {
    const scale = buildColourScale(metric({ direction: "neutral" }), SPREAD);
    expect(scale.note).toContain("no better or worse direction");
    expect(scale.note).not.toContain("worse end");
  });

  it("assigns classes monotonically and covers the whole domain", () => {
    const scale = buildColourScale(metric(), SPREAD);
    let previous = -1;
    for (const v of SPREAD) {
      const i = scale.classOf(v);
      expect(i).toBeGreaterThanOrEqual(previous);
      expect(i).toBeGreaterThanOrEqual(0);
      previous = i;
    }
    expect(scale.classOf(SPREAD[0])).toBe(0);
    expect(scale.classOf(SPREAD[SPREAD.length - 1])).toBe(6);
  });

  it("resists an extreme outlier, which equal intervals would not", () => {
    // The real case: City of London at 698 against a median of 113.
    const withOutlier = [...Array.from({ length: 32 }, (_, i) => 75 + i * 2), 698];
    const scale = buildColourScale(metric(), withOutlier);
    const used = new Set(withOutlier.map((v) => scale.classOf(v)));
    // Equal intervals would put 32 of 33 boroughs in class 0. Quantiles must
    // spread them across every class.
    expect(used.size).toBe(7);
  });
});

describe("buildColourScale : degenerate domains", () => {
  it("collapses to one class when every value is identical, and says why", () => {
    // imd_employment_score is published to one decimal place and has a single
    // distinct value across all 33 boroughs. This is real data, not a fixture.
    const scale = buildColourScale(metric({ scale: "proportion" }), new Array(33).fill(0.1));
    expect(scale.degenerate).toBe(true);
    expect(scale.classes).toHaveLength(1);
    expect(scale.note).toContain("same value");
    expect(scale.note).toContain("not a rendering fault");
    // A mid step, not the darkest: "no variation" is not "everywhere is the
    // worst", and a solid dark map says the second thing.
    expect(scale.classes[0].fill).toBe("var(--seq-400)");
  });

  it("reduces the class count when quantile breaks collapse", () => {
    const twoValues = [...new Array(20).fill(0.1), ...new Array(13).fill(0.2)];
    const scale = buildColourScale(metric({ scale: "proportion" }), twoValues);
    expect(scale.classes.length).toBeGreaterThan(1);
    expect(scale.classes.length).toBeLessThan(7);
    // Whatever the count, the ramp still spans light to dark.
    expect(scale.classes[0].fill).toBe("var(--seq-100)");
    expect(scale.classes[scale.classes.length - 1].fill).toBe("var(--seq-700)");
  });

  it("returns a no-data scale when there are no values at all", () => {
    const scale = buildColourScale(metric(), [null, null, null]);
    expect(scale.n).toBe(0);
    expect(scale.domain).toBeNull();
    expect(scale.fillOf(1)).toBe(NO_DATA_FILL);
  });
});

describe("buildColourScale : diverging", () => {
  const standardised = metric({ scale: "standardised", direction: "higher_is_worse" });

  it("uses the diverging ramp for standardised metrics", () => {
    const scale = buildColourScale(standardised, [-1.4, -0.4, 0.4]);
    expect(scale.ramp).toBe("diverging");
    expect(scale.classes).toHaveLength(7);
  });

  it("centres the midpoint on zero, not on the middle of the observed range", () => {
    // Observed range is −1.4 to +0.4, whose midpoint is −0.5. A quantile or
    // min–max scale would put the neutral colour there, which is wrong: zero is
    // the national average and the only meaningful midpoint.
    const scale = buildColourScale(standardised, [-1.4, -0.4, 0.4]);
    const middle = scale.classes[3];
    expect(middle.min).toBeLessThan(0);
    expect(middle.max).toBeGreaterThan(0);
    expect(middle.fill).toBe("var(--div-mid)");
    expect(scale.classOf(0)).toBe(3);
  });

  it("is symmetric about zero", () => {
    const scale = buildColourScale(standardised, [-1.4, -0.4, 0.4]);
    const lo = scale.classes[0].min;
    const hi = scale.classes[6].max;
    expect(lo).toBeCloseTo(-hi, 10);
  });

  it("puts the red pole on the worse side, whichever side that is", () => {
    const worse = buildColourScale(standardised, [-1, 1]);
    expect(worse.classes[6].fill).toBe("var(--div-high)");
    expect(worse.classes[0].fill).toBe("var(--div-low)");

    const better = buildColourScale(
      metric({ scale: "standardised", direction: "higher_is_better" }),
      [-1, 1],
    );
    expect(better.classes[6].fill).toBe("var(--div-low)");
    expect(better.classes[0].fill).toBe("var(--div-high)");
  });

  it("does not use quantile breaks, which would move the midpoint off zero", () => {
    // 30 values just below zero and 3 well above: quantiles would put four
    // breaks in the negative cluster and the midpoint with them.
    const skewed = [...new Array(30).fill(-0.1), 1.0, 1.1, 1.2];
    const scale = buildColourScale(standardised, skewed);
    expect(scale.classOf(0)).toBe(3);
  });
});

describe("no-data handling", () => {
  it("returns the hatch fill for null, undefined and non-finite values", () => {
    const scale = buildColourScale(metric(), SPREAD);
    expect(scale.fillOf(null)).toBe(NO_DATA_FILL);
    expect(scale.fillOf(undefined)).toBe(NO_DATA_FILL);
    expect(scale.fillOf(Number.NaN)).toBe(NO_DATA_FILL);
    // A pattern, not a colour: issue 3.8 requires the no-data state to be
    // distinguishable without relying on colour alone.
    expect(NO_DATA_FILL).toContain("url(#");
  });

  it("never returns the no-data fill for a value inside the domain", () => {
    const scale = buildColourScale(metric(), SPREAD);
    for (const v of SPREAD) expect(scale.fillOf(v)).not.toBe(NO_DATA_FILL);
  });
});

describe("palette discipline", () => {
  it("emits no raw hex anywhere : every fill is a token or a mix of tokens", () => {
    // globals.css sets this rule for everything from 3.2 onwards. A hex here
    // would look right in light mode and wrong in dark mode, silently.
    for (const id of Object.keys(coverage.metrics)) {
      const cov = coverage.metrics[id];
      const scale = buildColourScale(cov, SPREAD);
      for (const c of scale.classes) {
        expect(c.fill, `${id} class fill`).not.toMatch(/#[0-9a-f]{3,8}/i);
        expect(c.fill).toMatch(/var\(--/);
      }
    }
  });
});

describe("legendRows", () => {
  it("labels every class with its real break values", () => {
    const scale = buildColourScale(metric({ scale: "rate" }), SPREAD);
    const rows = legendRows(scale, "rate");
    expect(rows).toHaveLength(scale.classes.length);
    expect(rows[0].label).toMatch(/^1\.0 – </);
    // The top class is closed, not open-ended.
    expect(rows[rows.length - 1].label).not.toContain("<");
  });

  it("formats currency breaks as currency, not as bare numbers", () => {
    const scale = buildColourScale(metric({ scale: "currency" }), [29400, 35900, 70000]);
    expect(legendRows(scale, "currency")[0].label).toContain("£");
  });
});
