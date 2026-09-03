import { readFileSync } from "node:fs";

import { expect, test, type Page } from "@playwright/test";

/**
 * Every assertion here maps to an acceptance criterion on issue 3.1. They are
 * written to fail on the things that actually go wrong in a responsive shell:
 * horizontal overflow, a nav that vanishes without a replacement, touch targets
 * under 44px, and accessibility violations that a visual check never catches.
 */

// npm hoists to the workspace root, so resolve rather than guessing a path.
const AXE = readFileSync(require.resolve("axe-core/axe.min.js"), "utf8");

const WIDTHS = [
  { name: "mobile", width: 375, height: 812 },
  { name: "tablet", width: 768, height: 1024 },
  { name: "desktop", width: 1280, height: 900 },
] as const;

const ROUTES = ["/", "/insights", "/methodology"] as const;

type AxeResult = {
  violations: { id: string; impact: string | null; nodes: unknown[]; help: string }[];
};

async function audit(page: Page): Promise<AxeResult> {
  await page.addScriptTag({ content: AXE });
  return page.evaluate(async () => {
    // @ts-expect-error injected at runtime
    return (await window.axe.run(document, {
      // The rule sets Lighthouse's accessibility category scores against.
      runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"] },
    })) as AxeResult;
  });
}

for (const vp of WIDTHS) {
  test.describe(`${vp.name} : ${vp.width}px`, () => {
    test.use({ viewport: { width: vp.width, height: vp.height } });

    for (const route of ROUTES) {
      test(`${route} renders with no horizontal overflow`, async ({ page }) => {
        await page.goto(route);
        await expect(page.getByRole("heading", { level: 1 })).toBeVisible();

        // The single most common responsive failure, and invisible in a
        // screenshot taken at the wrong scroll position.
        const overflow = await page.evaluate(
          () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
        );
        expect(overflow, "horizontal overflow in px").toBeLessThanOrEqual(0);

        await expect(page.getByRole("banner")).toBeVisible();
        await expect(page.getByRole("main")).toBeVisible();
        await expect(page.getByRole("contentinfo")).toBeVisible();
      });

      test(`${route} passes axe (wcag2a/aa, wcag21a/aa)`, async ({ page }) => {
        await page.goto(route);
        const { violations } = await audit(page);
        expect(
          violations.map((v) => `${v.id} (${v.impact}) × ${v.nodes.length}: ${v.help}`),
        ).toEqual([]);
      });
    }

    test("navigation is reachable", async ({ page }) => {
      await page.goto("/");
      if (vp.width < 768) {
        // Links are behind a disclosure below the md breakpoint. What matters
        // is that they are reachable, not that they are always visible.
        const menu = page.getByRole("button", { name: "Menu" });
        await expect(menu).toBeVisible();
        await expect(menu).toHaveAttribute("aria-expanded", "false");
        await menu.click();
        await expect(menu).toHaveAttribute("aria-expanded", "true");
      }
      await expect(
        page.getByRole("link", { name: "Methodology" }).first(),
      ).toBeVisible();
    });

    test("interactive targets are at least 44px", async ({ page }) => {
      await page.goto("/");
      if (vp.width < 768) await page.getByRole("button", { name: "Menu" }).click();

      const targets = page.locator("header a, header button");
      const undersized: string[] = [];
      for (let i = 0; i < (await targets.count()); i++) {
        const el = targets.nth(i);
        if (!(await el.isVisible())) continue;
        const box = await el.boundingBox();
        if (box && Math.min(box.width, box.height) < 44) {
          undersized.push(`${(await el.innerText()).trim() || "(icon)"} ${box.width}×${box.height}`);
        }
      }
      expect(undersized).toEqual([]);
    });
  });
}

test.describe("theme", () => {
  test("the choice beats the OS setting in both directions", async ({ browser }) => {
    const ctx = await browser.newContext({ colorScheme: "dark" });
    const page = await ctx.newPage();
    await page.goto("/");

    // OS dark, no choice stored: the toggle reports dark.
    await expect(page.getByRole("button", { name: /switch to light theme/i })).toBeVisible();

    // Choosing light must win over OS dark : the case an automatic flip breaks.
    await page.getByRole("button", { name: /switch to light theme/i }).click();
    await expect(page.locator("html")).toHaveAttribute("data-theme", "light");

    // Persistence is a SEPARATE claim from the attribute, and it is the one the
    // reload depends on. Checking it here is not belt and braces: the toggle
    // stamps the attribute first and writes to storage second, so a failed write
    // leaves the assertion above passing and the one after the reload failing,
    // with nothing in the log connecting the two. That is precisely how this
    // test failed in CI and could not be diagnosed. Now a storage failure is
    // reported as a storage failure, at the line where it happens.
    const stored = await page.evaluate(() => {
      try {
        return { value: localStorage.getItem("theme"), error: null as string | null };
      } catch (e) {
        return { value: null, error: String(e) };
      }
    });
    expect(
      await page.locator("html").getAttribute("data-theme-persisted"),
      "the toggle reported that writing the theme to localStorage failed",
    ).toBeNull();
    expect(stored, "the theme choice was not written to localStorage").toEqual({
      value: "light",
      error: null,
    });

    await page.reload();
    await expect(page.locator("html")).toHaveAttribute("data-theme", "light");

    // The attribute alone only proves the inline head script ran. The toggle
    // offering the way back proves the choice also survived hydration, which is
    // the other way this could regress.
    await expect(page.getByRole("button", { name: /switch to dark theme/i })).toBeVisible();
    await ctx.close();
  });

  test("a browser that refuses storage degrades visibly, not silently", async ({ browser }) => {
    // The regression test for a CI failure that could not be reproduced locally.
    // The toggle stamps data-theme first and writes to localStorage second, and
    // the write was wrapped in a catch that swallowed everything. So a refused
    // write left the click looking successful and the choice gone on the next
    // load, reported as a missing attribute with no mention of storage.
    //
    // This pins the contract: the theme still changes for the session, and the
    // failure to persist it is recorded on the document where a test : or
    // someone reading a bug report : can see it.
    const ctx = await browser.newContext({ colorScheme: "dark" });
    await ctx.addInitScript(() => {
      const real = Storage.prototype.setItem;
      Storage.prototype.setItem = function (key: string, value: string) {
        if (key === "theme") throw new DOMException("blocked", "SecurityError");
        return real.call(this, key, value);
      };
    });
    const page = await ctx.newPage();
    await page.goto("/");
    await page.getByRole("button", { name: /switch to light theme/i }).click();

    // Still usable this session...
    await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
    // ...and the reason the next load will not remember it is on the record.
    await expect(page.locator("html")).toHaveAttribute("data-theme-persisted", "false");

    await page.reload();
    expect(await page.locator("html").getAttribute("data-theme")).toBeNull();
    await ctx.close();
  });
});

test.describe("content", () => {
  test("the footer attributes every publisher and both licences", async ({ page }) => {
    await page.goto("/");
    const footer = page.getByRole("contentinfo");
    await expect(footer).toContainText("Open Government Licence v3.0");
    await expect(footer).toContainText("Ordnance Survey");
    for (const publisher of ["Police", "ONS", "MHCLG", "HMRC"]) {
      await expect(footer, `attribution for ${publisher}`).toContainText(publisher);
    }
  });

  test("the shell shows real figures, not placeholders", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByRole("main")).toContainText("33");
    await expect(page.getByRole("main")).toContainText("2011");
  });

  test("partial borough coverage is stated, not left implicit", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByRole("contentinfo")).toContainText("City of London");
  });
});
