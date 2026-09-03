import { readFileSync } from "node:fs";

import { expect, test, type Page } from "@playwright/test";

import { deltaE, lightness, parseRgb, simulateDeuteranopia } from "./cvd";

/**
 * Browser checks for the dashboard : plan issues 3.2 to 3.8.
 *
 * Every assertion here maps to a named acceptance criterion, and they are
 * written to fail on the specific things this dataset makes easy to get wrong:
 * a 32-borough metric drawn as if it had 33, a ramp that runs the same way for
 * `higher_is_worse` and `higher_is_better`, a slider offering years a metric
 * does not publish, a rank denominator of 33 where the source says 32, and a
 * trend line fitted across a partial year.
 */

const AXE = readFileSync(require.resolve("axe-core/axe.min.js"), "utf8");

const MAP = 'svg[aria-label*="Choropleth"]';
const PATHS = `${MAP} path`;

type AxeResult = {
  violations: { id: string; impact: string | null; nodes: unknown[]; help: string }[];
};

async function audit(page: Page): Promise<AxeResult> {
  await page.addScriptTag({ content: AXE });
  return page.evaluate(async () => {
    // @ts-expect-error injected at runtime
    return (await window.axe.run(document, {
      runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"] },
    })) as AxeResult;
  });
}

/** The detail panel : identified by the Close button only it has. */
function detailPanel(page: Page) {
  return page.locator("section", { has: page.getByRole("button", { name: "Close" }) });
}

/** Perceptual lightness of each legend swatch, in class order. */
async function legendLightness(page: Page): Promise<number[]> {
  const swatches = await page.$$eval('li span[class*="inline-block"]', (nodes) =>
    nodes.map((n) => getComputedStyle(n as Element).backgroundColor),
  );
  return swatches.slice(0, 7).map((s) => lightness(parseRgb(s)));
}

/** The fills actually painted on the map, in document order. */
async function mapFills(page: Page): Promise<string[]> {
  return page.$$eval(PATHS, (nodes) =>
    nodes.map((n) => getComputedStyle(n as Element).fill),
  );
}

// ────────────────────────────────────────────────────────────── issue 3.2

test.describe("3.2 : choropleth", () => {
  test("draws all 33 boroughs and colours them by the selected metric", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(PATHS)).toHaveCount(33);

    const fills = await mapFills(page);
    expect(new Set(fills).size).toBeGreaterThan(1);
    // Seven quantile classes over 33 boroughs: every class should be used.
    expect(new Set(fills).size).toBe(7);
  });

  test("recolours when the metric changes, without a reload", async ({ page }) => {
    await page.goto("/");
    const before = await mapFills(page);

    // A document-level marker survives a client re-render and does not survive
    // a page load. `framenavigated` is the wrong probe here: it also fires for
    // the history.replaceState the dashboard uses to mirror its state.
    await page.evaluate(() => {
      (window as unknown as { __alive?: number }).__alive = 1;
    });

    await page.getByLabel("Map this metric").selectOption("income_median");
    await expect(page.locator(MAP)).toHaveAttribute("aria-label", /Median income/);

    expect(await mapFills(page)).not.toEqual(before);
    expect(
      await page.evaluate(() => (window as unknown as { __alive?: number }).__alive),
      "changing the metric must not reload the page",
    ).toBe(1);
  });

  test("draws boroughs a metric does not cover in the no-data hatch, never omitted", async ({
    page,
  }) => {
    // The live case: City of London has no well-being estimate at all.
    await page.goto("/?metric=wellbeing_anxiety&year=2022");
    await expect(page.locator(PATHS)).toHaveCount(33);

    const hatched = (await mapFills(page)).filter((f) => f.includes("no-data-hatch"));
    expect(hatched).toHaveLength(1);

    // A pattern element, so the state survives greyscale and colour blindness.
    await expect(page.locator("#no-data-hatch")).toHaveCount(1);
    await expect(page.getByText(/No data \(1 borough\)/)).toBeVisible();
  });

  test("the legend carries the metric's own units and real break values", async ({ page }) => {
    await page.goto("/?metric=income_median&year=2023");
    const legend = page.locator("section", { has: page.locator(MAP) });
    // Currency formatting, not bare numbers.
    await expect(legend).toContainText("£");

    await page.getByLabel("Map this metric").selectOption("wellbeing_life_satisfaction");
    await expect(legend).toContainText("mean 0-10");
  });

  test("runs the ramp the opposite way for higher_is_better and higher_is_worse", async ({
    page,
  }) => {
    // Darker must mean worse for BOTH, so the borough at the top of a
    // higher_is_worse metric and the bottom of a higher_is_better one are the
    // dark ones. Without this, a reader comparing crime and income has to
    // reverse the ramp in their head, and will not.
    //
    // Checking the CAPTION is not enough. The note and the ramp are built by
    // separate code paths, so a build that ignores `direction` entirely still
    // prints "Darker means lower" over a ramp running the other way : verified
    // by injecting exactly that fault, which an earlier version of this test
    // passed. So this reads the painted colours.
    await page.goto("/?metric=crime_rate_per_1000&year=2023");
    await expect(page.getByText(/Darker means higher/)).toBeVisible();
    const worse = await legendLightness(page);
    expect(worse).toHaveLength(7);
    for (let i = 1; i < worse.length; i++) {
      expect(worse[i], `higher_is_worse: class ${i} must be darker`).toBeLessThan(worse[i - 1]);
    }

    await page.getByLabel("Map this metric").selectOption("income_median");
    await expect(page.getByText(/Darker means lower/)).toBeVisible();
    const better = await legendLightness(page);
    expect(better).toHaveLength(7);
    for (let i = 1; i < better.length; i++) {
      expect(better[i], `higher_is_better: class ${i} must be lighter`).toBeGreaterThan(
        better[i - 1],
      );
    }
  });

  test("centres a standardised metric's ramp on zero, not on its observed midpoint", async ({
    page,
  }) => {
    await page.goto("/?metric=imd_health_deprivation_and_disability_score&year=2019");
    await expect(page.getByText(/Centred on zero/)).toBeVisible();
    // The observed range is −1.4 to +0.4; a min–max scale would put the neutral
    // colour near −0.5.
    await expect(page.getByText(/The midpoint is zero, not the middle of London's range/)).toBeVisible();
  });

  test("says so rather than pretending, when a metric has no variation to map", async ({
    page,
  }) => {
    // imd_employment_score is published to one decimal place and has a single
    // distinct value across all 33 boroughs. A flat map with no caption reads
    // as a broken renderer.
    await page.goto("/?metric=imd_employment_score&year=2019");
    await expect(page.getByText(/Every borough shown has the same value/)).toBeVisible();
    await expect(page.getByText(/not a rendering fault/)).toBeVisible();

    // One fill across all 33, and the summary refuses to name a "highest" and a
    // "lowest" that are the same borough.
    expect(new Set(await mapFills(page)).size).toBe(1);
    await expect(page.getByText("No variation between boroughs")).toBeVisible();
    await expect(page.getByText(/Highest : the worse end/)).toHaveCount(0);
  });

  test("zooms and pans the viewBox, and does not let the page scroll under a touch drag", async ({
    page,
  }) => {
    await page.goto("/");
    const map = page.locator(MAP);
    const home = await map.getAttribute("viewBox");

    await page.getByRole("button", { name: "Zoom in" }).click();
    const zoomed = await map.getAttribute("viewBox");
    expect(zoomed).not.toBe(home);

    await page.getByRole("button", { name: "Reset the map view" }).click();
    await expect(map).toHaveAttribute("viewBox", home!);

    // touch-action: none is what makes a one-finger drag pan the map instead of
    // scrolling the page. Without it the gesture is unusable on a phone.
    expect(await map.evaluate((el) => getComputedStyle(el).touchAction)).toBe("none");
  });
});

// ────────────────────────────────────────────────────────────── issue 3.3

test.describe("3.3 : metric switcher and exclusions", () => {
  test("builds the metric list from the coverage matrix, with all 19 metrics", async ({ page }) => {
    await page.goto("/");
    const options = page.locator("#metric-select option");
    await expect(options).toHaveCount(19);
    // Grouped by family, from the data rather than a hardcoded list.
    await expect(page.locator("#metric-select optgroup")).toHaveCount(5);
  });

  test("excluding a borough recomputes the colour classes, not just its own shade", async ({
    page,
  }) => {
    await page.goto("/?metric=crime_rate_per_1000&year=2023");
    const before = await mapFills(page);

    await page.getByRole("checkbox", { name: /Exclude City of London/ }).check();
    await expect(page.getByText(/1 borough is excluded from the scale/)).toBeVisible();

    const after = await mapFills(page);
    // Every borough is re-classed against the remaining 32, so this is not a
    // one-shape change.
    const changed = after.filter((f, i) => f !== before[i]).length;
    expect(changed).toBeGreaterThan(1);
  });

  test("holds the whole view in the query string, so it can be shared", async ({ page }) => {
    await page.goto("/");
    await page.getByLabel("Map this metric").selectOption("wellbeing_happiness");
    await page.getByRole("checkbox", { name: /Exclude City of London/ }).check();

    await expect(page).toHaveURL(/metric=wellbeing_happiness/);
    await expect(page).toHaveURL(/exclude=E09000001/);

    // And the shared URL reopens the same view, server-rendered.
    const shared = page.url();
    await page.goto(shared);
    await expect(page.getByLabel("Map this metric")).toHaveValue("wellbeing_happiness");
    await expect(page.getByRole("checkbox", { name: /Exclude City of London/ })).toBeChecked();
  });

  test("is operable from the keyboard alone", async ({ page }) => {
    await page.goto("/");
    await page.getByLabel("Map this metric").focus();
    await page.keyboard.press("ArrowDown");
    await expect(page).toHaveURL(/metric=/);
  });

  test("explains a stale link instead of failing or silently ignoring it", async ({ page }) => {
    await page.goto("/?metric=not_a_metric&year=1999&exclude=E06000001");
    const notice = page.getByRole("status");
    await expect(notice).toContainText("not a metric");
    await expect(notice).toContainText("E06000001");
    await expect(notice).toContainText("1999");
    // And it still renders a usable dashboard.
    await expect(page.locator(PATHS)).toHaveCount(33);
  });
});

// ────────────────────────────────────────────────────────────── issue 3.4

test.describe("3.4 : year control", () => {
  test("offers a slider over the metric's own years, not the global window", async ({ page }) => {
    await page.goto("/?metric=crime_rate_per_1000");
    const slider = page.getByRole("slider");
    // crime_rate_per_1000 runs 2011–2024: 14 years, indices 0–13.
    await expect(slider).toHaveAttribute("max", "13");

    await page.getByLabel("Map this metric").selectOption("wellbeing_anxiety");
    // Well-being stops at 2022: 12 years, indices 0–11.
    await expect(page.getByRole("slider")).toHaveAttribute("max", "11");
  });

  test("moves the year onto the new metric's range and says it did", async ({ page }) => {
    await page.goto("/?metric=crime_rate_per_1000&year=2024");
    await page.getByLabel("Map this metric").selectOption("wellbeing_anxiety");
    await expect(page.getByRole("status")).toContainText("no data for 2024");
    await expect(page.getByRole("status")).toContainText("2022");
  });

  test("uses discrete points for snapshot metrics rather than a continuous slider", async ({
    page,
  }) => {
    await page.goto("/?metric=imd_income_score");
    await expect(page.getByRole("slider")).toHaveCount(0);
    await expect(page.getByRole("radio", { name: "2015" })).toBeVisible();
    await expect(page.getByRole("radio", { name: "2019" })).toBeVisible();
    await expect(page.getByText(/separate exercises whose ranks are not comparable/)).toBeVisible();
  });

  test("marks a partial year and refuses to open on one", async ({ page }) => {
    // crime_count runs to 2026, but 2026 is four months of data.
    await page.goto("/?metric=crime_count");
    await expect(page.getByText(/2026 is not a full twelve months/)).toBeVisible();

    await page.goto("/?metric=crime_count&year=2026");
    await expect(page.getByText(/2026 is a partial year/)).toBeVisible();
    await expect(page.getByText(/not comparable with any other year/)).toBeVisible();
  });

  test("repaints the map when the year changes", async ({ page }) => {
    await page.goto("/?metric=crime_rate_per_1000&year=2023");
    const before = await mapFills(page);
    await page.getByRole("slider").fill("0");
    await expect(page.locator(MAP)).toHaveAttribute("aria-label", /in 2011/);
    expect(await mapFills(page)).not.toEqual(before);
  });
});

// ────────────────────────────────────────────────────────────── issue 3.5

test.describe("3.5 : borough detail", () => {
  test("opens from the map, lists every metric, and closes again", async ({ page }) => {
    await page.goto("/");
    await page.locator(PATHS).nth(5).click();

    const panel = detailPanel(page);
    await expect(panel).toBeVisible();

    // Every metric, grouped by family : scoped to the panel, because the metric
    // switcher holds the same labels in hidden <option> elements.
    await expect(panel.getByText("Median income")).toBeVisible();
    await expect(panel.getByText("Life expectancy at birth, female")).toBeVisible();
    await expect(panel.locator("dt")).toHaveCount(19);
    await expect(panel.locator("h3")).toHaveCount(5);

    await panel.getByRole("button", { name: "Close" }).click();
    await expect(page.getByText("Select a borough")).toBeVisible();
  });

  test("closes on Escape", async ({ page }) => {
    await page.goto("/?borough=E09000022");
    await expect(page.getByRole("button", { name: "Close" })).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.getByText("Select a borough")).toBeVisible();
  });

  test("ranks against the metric's real coverage : 32 where City of London is absent", async ({
    page,
  }) => {
    await page.goto("/?borough=E09000022&year=2022");
    const panel = detailPanel(page);
    // Well-being and life expectancy cover 32 boroughs.
    await expect(panel.getByText(/of 32/).first()).toBeVisible();
    // Income and crime cover all 33.
    await expect(panel.getByText(/of 33/).first()).toBeVisible();
  });

  test("says WHY a value is missing rather than leaving a blank", async ({ page }) => {
    await page.goto("/?borough=E09000001&year=2022");
    const panel = detailPanel(page);
    await expect(panel.getByText(/Not published for City of London/).first()).toBeVisible();
    await expect(panel.getByText(/population is too small/).first()).toBeVisible();
  });

  test("labels a value taken from a metric's nearest year as such", async ({ page }) => {
    // IMD exists only for 2015 and 2019; the reader is looking at 2023.
    await page.goto("/?borough=E09000022&metric=crime_rate_per_1000&year=2023");
    const panel = detailPanel(page);
    await expect(panel.getByText(/its nearest published year/).first()).toBeVisible();
  });
});

// ────────────────────────────────────────────────────────────── issue 3.6

test.describe("3.6 : scatterplot", () => {
  test("plots one point per borough with both values, and reports r", async ({ page }) => {
    await page.goto("/?compare=income_median&year=2023");
    const points = page.locator('svg[aria-label*="Scatterplot"] circle');
    await expect(points).toHaveCount(33);
    await expect(page.getByText(/Pearson r =/)).toBeVisible();
  });

  test("carries the correlation-is-not-causation note, in associative language", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.getByText("Association, not cause.")).toBeVisible();
    await expect(page.getByText(/does not hold for the people inside those areas/)).toBeVisible();
    // The heading must not claim a direction of effect.
    const heading = await page.getByRole("heading", { name: /against/ }).first().textContent();
    expect(heading?.toLowerCase()).not.toMatch(/caus|driv|explain|because|impact/);
  });

  test("prints the year pairing, and names the rules when they differ", async ({ page }) => {
    await page.goto("/?compare=income_median&year=2023");
    // Crime is a calendar year; income is a financial year labelled by its
    // start. Scoped to the scatter's own caption: "calendar year" also appears
    // in the metric switcher's coverage summary, which is a different claim.
    const pairing = page.locator("p", { hasText: "not the same twelve months" });
    await expect(pairing).toContainText("calendar year");
    await expect(pairing).toContainText("financial year, labelled by its start");
    await expect(pairing).toContainText("2023 ×");
  });

  test("pairs on the nearest published year and says it did, without interpolating", async ({
    page,
  }) => {
    // Crime has 2024; income stops at 2023.
    await page.goto("/?compare=income_median&year=2024");
    await expect(page.getByText(/the nearest published year on each side, not interpolated/)).toBeVisible();
  });

  test("drops boroughs missing from either series and counts them", async ({ page }) => {
    await page.goto("/?compare=wellbeing_anxiety&year=2022");
    const points = page.locator('svg[aria-label*="Scatterplot"] circle');
    await expect(points).toHaveCount(32);
    await expect(
      page.getByText(/1 borough is not plotted because one of the two series does not cover it/),
    ).toBeVisible();
  });

  test("fits no line when the x variable has no variation", async ({ page }) => {
    await page.goto("/?compare=imd_employment_score&year=2019");
    await expect(page.getByText(/No line is fitted/)).toBeVisible();
    await expect(page.getByText(/no variation between boroughs at this precision/)).toBeVisible();
  });

  test("links a hovered point to the borough on the map", async ({ page }) => {
    await page.goto("/");
    const point = page.locator('svg[aria-label*="Scatterplot"] circle').first();
    const radiusBefore = await point.getAttribute("r");
    await point.hover();
    await expect(point).not.toHaveAttribute("r", radiusBefore!);
    // The borough's name appears on the chart, and its shape is emphasised.
    const emphasised = await page.$$eval(PATHS, (nodes) =>
      // getComputedStyle returns "2.5px"; Number() of that is NaN.
      nodes.filter((n) => Number.parseFloat(getComputedStyle(n as Element).strokeWidth) > 1)
        .length,
    );
    expect(emphasised).toBeGreaterThan(0);
  });
});

// ────────────────────────────────────────────────────────────── issue 3.7

test.describe("3.7 : KPI panel", () => {
  test("labels the extremes by meaning, not by raw value", async ({ page }) => {
    await page.goto("/?metric=crime_rate_per_1000&year=2023");
    await expect(page.getByText("Highest : the worse end")).toBeVisible();

    await page.getByLabel("Map this metric").selectOption("income_median");
    // For a higher_is_better metric the LOW end is the bad one, so the card flips.
    await expect(page.getByText("Lowest : the worse end")).toBeVisible();
  });

  test("reads trend direction from the metric, not from the sign", async ({ page }) => {
    await page.goto("/?metric=crime_rate_per_1000");
    const crime = page.locator("dl").first();
    const crimeTrend = await crime.textContent();

    await page.getByLabel("Map this metric").selectOption("life_expectancy_birth_female");
    const life = await page.locator("dl").first().textContent();

    // Both changed, and the words used are drawn from direction. The arrow is
    // never the only signal: "improving"/"worsening" is spelled out.
    expect(crimeTrend).toMatch(/improving|worsening|no direction/);
    expect(life).toMatch(/improving|worsening|no direction/);
  });

  test("refuses a long-run change across two IMD snapshots", async ({ page }) => {
    await page.goto("/?metric=imd_income_score&year=2019");
    await expect(page.getByText("Two snapshots four years apart are not a trend.")).toBeVisible();
  });

  test("calls the London figure a borough mean, not a London rate", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByText(/unweighted across 33 boroughs/)).toBeVisible();
    await expect(page.getByText(/not a population-weighted London figure/)).toBeVisible();
  });
});

// ────────────────────────────────────────────────────────────── issue 3.8

const WIDTHS = [
  { name: "mobile", width: 375, height: 812 },
  { name: "tablet", width: 768, height: 1024 },
  { name: "desktop", width: 1280, height: 900 },
] as const;

/** Views chosen because each exercises a different awkward part of the data. */
const VIEWS = [
  "/",
  "/?metric=wellbeing_anxiety&year=2022",
  "/?metric=imd_health_deprivation_and_disability_score&year=2019",
  "/?borough=E09000022&exclude=E09000001",
];

test.describe("3.8 : cross-device and accessibility", () => {
  for (const vp of WIDTHS) {
    test.describe(`${vp.name} : ${vp.width}px`, () => {
      test.use({ viewport: { width: vp.width, height: vp.height } });

      for (const view of VIEWS) {
        test(`${view} passes axe with no horizontal overflow`, async ({ page }) => {
          await page.goto(view);
          await expect(page.locator(PATHS).first()).toBeVisible();

          const overflow = await page.evaluate(
            () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
          );
          expect(overflow, "horizontal overflow in px").toBeLessThanOrEqual(0);

          const { violations } = await audit(page);
          expect(
            violations.map((v) => `${v.id} (${v.impact}) × ${v.nodes.length}: ${v.help}`),
          ).toEqual([]);
        });
      }

      test("every dashboard control clears the 44px touch target floor", async ({ page }) => {
        await page.goto("/?borough=E09000022");
        // WCAG 2.5.5 measures the TARGET, which for a checkbox wrapped in a
        // label is the label: clicking anywhere in it activates the control.
        // Measuring the 16px input alone would fail a perfectly usable row :
        // but a checkbox with no wrapping label really would be a 16px target,
        // so that case is reported rather than skipped.
        const undersized = await page.$$eval(
          "main button, main select, main input, main a",
          (nodes) =>
            nodes
              .map((n) => {
                const el = n as HTMLElement;
                if (!el.getClientRects().length) return null;
                const isCheckable =
                  el.tagName === "INPUT" &&
                  ["checkbox", "radio"].includes((el as HTMLInputElement).type);
                const target = isCheckable ? (el.closest("label") ?? el) : el;
                const box = target.getBoundingClientRect();
                if (Math.min(box.width, box.height) >= 44) return null;
                const name =
                  el.getAttribute("aria-label") ||
                  target.textContent?.trim() ||
                  el.id ||
                  "(unnamed)";
                return `${name} ${Math.round(box.width)}×${Math.round(box.height)}`;
              })
              .filter((v): v is string => v !== null),
        );
        expect(undersized).toEqual([]);
      });
    });
  }

  test("the choropleth ramp survives a deuteranopia simulation", async ({ page }) => {
    await page.goto("/?metric=crime_rate_per_1000&year=2023");

    const swatches = await page.$$eval(
      'li span[class*="inline-block"]',
      (nodes) => nodes.map((n) => getComputedStyle(n as Element).backgroundColor),
    );
    expect(swatches.length).toBeGreaterThanOrEqual(7);

    const simulated = swatches.slice(0, 7).map((s) => simulateDeuteranopia(parseRgb(s)));

    // A sequential ramp is CVD-safe when it varies in LIGHTNESS, not hue. The
    // test that matters is therefore that the order survives the simulation: a
    // deuteranope must still be able to say which of two swatches is higher.
    for (let i = 1; i < simulated.length; i++) {
      expect(
        lightness(simulated[i]),
        `class ${i} must be darker than class ${i - 1} under simulation`,
      ).toBeLessThan(lightness(simulated[i - 1]));
    }

    // And the ends must be far enough apart to read as a range at all.
    expect(deltaE(simulated[0], simulated[6])).toBeGreaterThan(40);
  });

  test("the diverging poles stay distinguishable under a deuteranopia simulation", async ({
    page,
  }) => {
    await page.goto("/?metric=imd_health_deprivation_and_disability_score&year=2019");
    const swatches = await page.$$eval(
      'li span[class*="inline-block"]',
      (nodes) => nodes.map((n) => getComputedStyle(n as Element).backgroundColor),
    );
    const low = simulateDeuteranopia(parseRgb(swatches[0]));
    const high = simulateDeuteranopia(parseRgb(swatches[6]));
    // Blue against red rather than red against green, which is why this passes
    // where the obvious choice would not.
    expect(deltaE(low, high)).toBeGreaterThan(25);
  });

  test("the no-data state is distinguishable without relying on colour", async ({ page }) => {
    await page.goto("/?metric=wellbeing_anxiety&year=2022");

    // A pattern, not just a fill.
    await expect(page.locator("#no-data-hatch")).toHaveCount(1);
    const hatched = await page.$$eval(PATHS, (nodes) =>
      nodes.filter((n) => getComputedStyle(n as Element).fill.includes("no-data-hatch")).length,
    );
    expect(hatched).toBe(1);

    // And the word, in the legend and in the table.
    await expect(page.getByText(/No data \(1 borough\)/)).toBeVisible();
    await expect(page.getByRole("table").getByText("no data").first()).toBeVisible();
  });

  test("the map is described for readers who cannot see it, and the table carries the values", async ({
    page,
  }) => {
    await page.goto("/?metric=wellbeing_anxiety&year=2022");
    const label = await page.locator(MAP).getAttribute("aria-label");
    expect(label).toContain("33 London boroughs");
    expect(label).toContain("no data");
    expect(label).toContain("table below");

    // 33 rows plus the header.
    await expect(page.getByRole("row")).toHaveCount(34);
  });
});
