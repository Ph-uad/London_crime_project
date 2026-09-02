/**
 * Screenshot the dashboard at the three reference widths, in both themes, and
 * in the states that exercise the awkward parts of the data: the diverging
 * ramp, a 32-borough metric, an exclusion, and the detail panel.
 *
 *   npm run start -- --port 3210
 *   PW_CHROMIUM_PATH=... node shot.mjs [outdir]
 */
import { mkdirSync } from "node:fs";

import { chromium } from "@playwright/test";

const OUT = process.argv[2] ?? "/tmp/shots";
const BASE = process.env.SHOT_BASE ?? "http://127.0.0.1:3210";
mkdirSync(OUT, { recursive: true });

const browser = await chromium.launch({ executablePath: process.env.PW_CHROMIUM_PATH });

const WIDTHS = [
  ["375-mobile", 375, 900],
  ["768-tablet", 768, 1000],
  ["1280-desktop", 1280, 900],
];

/** Each of these is a case the map has to get right, not a pretty picture. */
const STATES = [
  ["dashboard", "/"],
  ["diverging-imd-health", "/?metric=imd_health_deprivation_and_disability_score&year=2019"],
  ["32-borough-wellbeing", "/?metric=wellbeing_anxiety&year=2022"],
  ["excluded-city", "/?exclude=E09000001"],
  ["detail-panel", "/?borough=E09000022&metric=income_median&year=2023"],
  ["flat-imd-employment", "/?metric=imd_employment_score&year=2019"],
];

for (const [name, w, h] of WIDTHS) {
  for (const theme of ["light", "dark"]) {
    const ctx = await browser.newContext({
      viewport: { width: w, height: h },
      colorScheme: theme,
      deviceScaleFactor: 2,
    });
    const page = await ctx.newPage();
    await page.goto(`${BASE}/`);
    await page.waitForLoadState("networkidle");
    await page.screenshot({ path: `${OUT}/${name}-${theme}.png`, fullPage: true });
    await ctx.close();
  }
}

for (const [name, path] of STATES) {
  const ctx = await browser.newContext({
    viewport: { width: 1280, height: 900 },
    deviceScaleFactor: 2,
  });
  const page = await ctx.newPage();
  await page.goto(`${BASE}${path}`);
  await page.waitForLoadState("networkidle");
  await page.screenshot({ path: `${OUT}/state-${name}.png`, fullPage: true });
  await ctx.close();
}

for (const route of ["/insights", "/methodology"]) {
  const ctx = await browser.newContext({
    viewport: { width: 1280, height: 900 },
    deviceScaleFactor: 2,
  });
  const page = await ctx.newPage();
  await page.goto(`${BASE}${route}`);
  await page.waitForLoadState("networkidle");
  await page.screenshot({ path: `${OUT}${route.replace("/", "/")}-1280.png`, fullPage: true });
  await ctx.close();
}

await browser.close();
console.log(`wrote screenshots to ${OUT}`);
