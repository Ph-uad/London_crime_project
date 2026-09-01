# Web Frontend

Next.js app (App Router, TypeScript) serving both the dashboard and the data API.

## Data API

| Route | Serves | Issue |
|---|---|---|
| `GET /api/metrics` | borough × year × metric × value observations | 2.1 |
| `GET /api/meta` | the coverage matrix | 2.1 |
| `GET /api/geo` | the 33 borough polygons, EPSG:4326 / RFC 7946 | 2.2 |

### Filtering `/api/metrics`

All optional, all comma-separated, all AND-ed:

```
/api/metrics?metric=crime_rate_per_1000,income_median
/api/metrics?year=2019,2020
/api/metrics?borough=E09000007
```

Boroughs are filtered by **GSS code**, not name. An unknown metric, year or
borough returns **400** with the valid values. So does an unknown parameter —
`?metrics=` (plural) is rejected rather than ignored, because silently returning
the whole dataset looks like it worked.

Every `/api/metrics` response carries the coverage entry for the metrics it
returned, so `partial_years` travels with the data instead of requiring a second
call to `/api/meta` that a caller might skip.

### Read `/api/meta` before rendering anything

The coverage matrix is the contract that stops the frontend guessing. Per metric:

| Field | Why it matters |
|---|---|
| `years`, `partial_years` | slider range; partial years are excluded from year-on-year comparison |
| `cadence` | `snapshot` (IMD) is discrete points, `annual` is a slider |
| `direction` | **anxiety and crime are `higher_is_worse`** — most other metrics are not |
| `scale`, `unit` | IMD domains span proportion, score and standardised; one colour ramp across them is wrong |
| `year_rule` | `calendar`, `financial_start`, `rolling_end` or `snapshot` — needed to state a cross-metric pairing honestly |
| `boroughs_missing` | City of London has no well-being and no life expectancy: those metrics cover **32** boroughs, not 33 |

## Where the data comes from

Three modules, split by what may reach the browser:

| Module | Holds | Client-safe? |
|---|---|---|
| `lib/coverage.ts` | `coverage.json` — labels, units, direction, cadence, years, per-metric borough coverage (9 KB) | **yes** |
| `lib/data.ts` | `boroughs.json` — all 6,001 observations (516 KB) | **no** — `server-only` |
| `lib/geo.ts` | `london.geojson`, and the projection into SVG paths | **no** — `server-only` |

`boroughs.json` and `coverage.json` are imported through the `@data/*` alias
(tsconfig → `../data/processed/*`) and bundled at build time; `london.geojson` is
read from disk and traced into the bundle by `next.config.ts`. Data changes only
on a redeploy, which is what the long `s-maxage` assumes.

**Why `lib/data.ts` is marked `server-only`,** given it has no filesystem access:
`components/site-header.tsx` is a client component, it imports `lib/site.ts`, and
`lib/site.ts` used to import `lib/data.ts` for the borough count. Turbopack could
not drop the JSON — the module derives `observations` and `allYears` from it at
module scope — so every visitor downloaded all 6,001 observations in order to
render the word "Dashboard" in the header. Confirmed by grepping the built chunk
and finding 6,001 `{"borough_gss":…}` objects; client JS fell from 1.2 MB to
716 KB once it was split. The marker turns a repeat of that from a silent half
megabyte into a build failure naming the cause.

The dashboard itself never reads any of these from the client. The server builds
a **compact index** — `metric → year → value[boroughIndex]`, see `lib/series.ts` —
and passes it down as a prop.

If the pipeline has not been run, `/api/geo` returns **503** naming the script to
run. The other two routes would fail the build, since their data is imported.

## Commands

Run these **from the repository root**. The root `package.json` delegates each one
to this workspace, so you do not have to remember `--workspace web`.

```bash
npm ci              # the lockfile lives at the root, not here
npm run dev         # next dev
npm run test        # route tests (vitest)
npm run typecheck
npm run lint
npm run build
npm run check       # lint + typecheck + test + build, i.e. what CI runs
npm run e2e --workspace web   # browser + accessibility checks (needs Chromium)
```

`npm run e2e` needs a browser: `npx playwright install chromium` once. In a
container that already ships one, point `PW_CHROMIUM_PATH` at it instead of
downloading.

Running them from inside `web/` works too. What does **not** work is a bare
`npm run <script>` at the root without those delegating scripts — the root
package is workspace-only and has no `next` or `vitest` of its own.

## The dashboard

One route (`/`), one client component tree, all state in the query string:

```
/?metric=crime_rate_per_1000&year=2023&compare=income_median&exclude=E09000001&borough=E09000007
```

The initial state is parsed **on the server** from the request's search params, so
a shared link renders correctly in the first HTML. Updates are mirrored with
`history.replaceState` rather than a router navigation — the trade is that the
back button does not step through metric changes, which for a dashboard is the
right way round.

Unknown *parameters* are ignored (a page URL collects `utm_source` from anything
that links to it); unknown *values* fall back to the default and are reported in a
visible notice. That is deliberately the opposite of `lib/http.ts`, which rejects
unknown API parameters outright.

**No mapping or charting library.** The choropleth is inline SVG over
hand-written Web Mercator (`lib/projection.ts`), the scales and class breaks are
in `lib/scales.ts`, and the correlation and fit are in `lib/stats.ts` — about
400 lines, all unit-tested, against roughly 250 KB of MapLibre + d3. The cost is
that there is no basemap: pan and zoom operate on the SVG viewBox, so a reader
cannot zoom in to see streets. For 33 borough polygons that is the right trade.
See plan issue 3.2.

The map is `role="img"`. The **borough table** beneath it is the keyboard and
screen-reader path, and carries the same values, selection and no-data states.

## Layout and design tokens

`app/globals.css` holds the palette as CSS custom properties — surfaces, ink,
categorical, sequential, diverging, status, and a no-data colour. **Charts from
3.2 onwards read these roles and must not introduce raw hex.** Three things
about them are load-bearing:

- The categorical slots were checked with a palette validator, not by eye:
  all-pairs in both modes, worst CVD ΔE 9.2 light / 9.4 dark against a ≥8
  target. The set is capped at three, because a fourth puts yellow beside
  orange and fails the floor for scatter and choropleth.
- `--text-muted` is **chart chrome only** — 3.50:1 on the light surface, under
  the 4.5:1 AA floor. Prose and captions use `--text-secondary` (7.73:1). Using
  muted for body text is what the axe run caught first time.
- Dark mode is *selected*: its own steps chosen for the dark surface, not an
  automatic inversion. A stored choice beats the OS setting in both directions.

## Tests

| Suite | What it covers | Count |
|---|---|---|
| `tests/api.test.ts` | the three routes, called directly with `Request` objects | 28 |
| `tests/projection.test.ts` | Web Mercator against the closed form, fitting, path building | 17 |
| `tests/geo.test.ts` | the real GeoJSON: 33 shapes, join by GSS code, geography sanity | 11 |
| `tests/series.test.ts` | the compact index, nearest-year pairing, absence reasons | 19 |
| `tests/scales.test.ts` | direction, quantile vs diverging classing, degenerate domains | 19 |
| `tests/stats.test.ts` | rank denominators, OLS and *r*, partial-year handling | 26 |
| `tests/url-state.test.ts` | parse, fall back, round-trip every metric | 19 |
| `e2e/shell.spec.ts` | shell + axe at 375/768/1280 on every route | 28 |
| `e2e/dashboard.spec.ts` | every 3.2–3.8 acceptance criterion, plus axe and CVD simulation | 53 |

139 unit, 81 browser. The browser suite runs against a real production build.

Two guards were checked by deliberately breaking the code: ignoring `direction`
in the ramp, and ranking against 33 boroughs regardless of coverage. The first
exposed a weak test — the browser check was reading the legend caption, which is
generated separately from the fills, so it passed on the broken build. It now
reads the painted colours.

## Status

Epics 2 and 3 are complete. Next: narrative content (4.1), Vercel deployment
(0.4), CI workflow installation (0.3).
