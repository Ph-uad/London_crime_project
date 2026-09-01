import { defineConfig, devices } from "@playwright/test";

/**
 * Browser checks for the layout shell (plan issue 3.1).
 *
 * Two of that issue's acceptance criteria are measurable and were previously
 * only assertable by eye: that the shell renders at 375 / 768 / 1280px, and
 * that it clears the Lighthouse accessibility bar. These run against a real
 * production build, so what is tested is what ships.
 */
export default defineConfig({
  testDir: "./e2e",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? "line" : "list",
  use: {
    baseURL: "http://127.0.0.1:3210",
    ...devices["Desktop Chrome"],
    // Normally Playwright uses the browser it downloaded. Some CI images and
    // dev containers ship one already; point PW_CHROMIUM_PATH at it to skip the
    // download rather than pinning a version here.
    launchOptions: process.env.PW_CHROMIUM_PATH
      ? { executablePath: process.env.PW_CHROMIUM_PATH }
      : {},
  },
  webServer: {
    command: "npm run build && npm run start -- --port 3210",
    url: "http://127.0.0.1:3210",
    // Never reuse. A `next start` left running from an earlier session answers
    // on this port with an OLD build, and the suite then reports failures
    // against code that is no longer on disk — which cost real time once
    // already. Always paying for a build is cheaper than debugging a ghost.
    reuseExistingServer: false,
    timeout: 180_000,
  },
});
