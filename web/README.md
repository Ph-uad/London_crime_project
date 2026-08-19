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

`web/lib/data.ts` is the only module that knows. `boroughs.json` and
`coverage.json` are imported through the `@data/*` alias (tsconfig →
`../data/processed/*`) and bundled at build time; `london.geojson` is read from
disk and traced into the bundle by `next.config.ts`. Data changes only on a
redeploy, which is what the long `s-maxage` assumes.

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
```

Running them from inside `web/` works too. What does **not** work is a bare
`npm run <script>` at the root without those delegating scripts — the root
package is workspace-only and has no `next` or `vitest` of its own.

## Status

- Done: data API (issues 2.1, 2.2), 28 route tests in CI.
- Next: layout shell (3.1), choropleth (3.2), controls and charts (3.3–3.7).
