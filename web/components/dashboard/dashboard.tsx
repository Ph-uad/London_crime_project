"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { BoroughDetail } from "@/components/dashboard/borough-detail";
import { BoroughTable } from "@/components/dashboard/borough-table";
import { Choropleth } from "@/components/dashboard/choropleth";
import { KpiPanel } from "@/components/dashboard/kpi-panel";
import { Legend } from "@/components/dashboard/legend";
import { MetricControls } from "@/components/dashboard/metric-controls";
import { Scatterplot } from "@/components/dashboard/scatterplot";
import { YearControl } from "@/components/dashboard/year-control";
import { describeYear } from "@/lib/format";
import { buildColourScale } from "@/lib/scales";
import { rowFor, type DashboardData } from "@/lib/series";
import { snapYear, toSearchString, type DashboardState } from "@/lib/url-state";
import type { CoverageMatrix } from "@/lib/types";

/**
 * The dashboard (plan issues 3.2–3.7).
 *
 * STATE. One object, held here, mirrored into the query string. The mirror is
 * written with `history.replaceState` rather than the router, because a router
 * navigation re-runs the route on every drag of the year slider for a value
 * that is entirely client-side. The trade this makes is explicit: the back
 * button does not step through metric changes. For a dashboard whose whole
 * state is one shareable URL that is the right way round : nobody expects Back
 * to undo a slider : and the URL stays copy-pasteable, which is what issue 3.3
 * asks for.
 *
 * The initial state is parsed on the SERVER from the request's search params, so
 * a shared link renders correctly in the first HTML rather than flashing the
 * default view and then correcting itself.
 *
 * DATA. Everything arrives as props. This file and its children never import
 * `@/lib/data`: that module imports the 516 KB borough export, and reaching it
 * from a client component would put the whole thing in the browser bundle. The
 * server builds a compact index instead : see `lib/series.ts`.
 */
export function Dashboard({
  data,
  coverage,
  initial,
  rejected,
}: {
  data: DashboardData;
  coverage: CoverageMatrix;
  initial: DashboardState;
  rejected: string[];
}) {
  const [state, setState] = useState<DashboardState>(initial);
  const [hovered, setHovered] = useState<number | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const { boroughs, metrics, series, shapes, viewBox } = data;
  const metric = metrics[state.metric];
  const excluded = useMemo(() => new Set(state.exclude), [state.exclude]);

  /**
   * The URL write is debounced; the render is not. Dragging the year slider
   * repaints immediately and writes one history entry when the drag settles,
   * which is what issue 3.4's "debounced updates" and "map updates ≤ 150 ms
   * after release" mean in practice.
   */
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => {
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => {
      const query = toSearchString(state, coverage);
      window.history.replaceState(null, "", `${window.location.pathname}${query}`);
    }, 150);
    return () => {
      if (timer.current) clearTimeout(timer.current);
    };
  }, [state, coverage]);

  const selectMetric = useCallback(
    (id: string) => {
      setState((prev) => {
        const next = metrics[id];
        const year = snapYear(next, prev.year);
        // Switching to a metric whose series stops earlier moves the year. Say
        // so, rather than letting the reader wonder why the map jumped.
        setNotice(
          year === prev.year
            ? null
            : `${next.label} has no data for ${prev.year}; showing ${describeYear(year, next)}.`,
        );
        return { ...prev, metric: id, year };
      });
    },
    [metrics],
  );

  const toggleExclude = useCallback((gss: string) => {
    setState((prev) => ({
      ...prev,
      exclude: prev.exclude.includes(gss)
        ? prev.exclude.filter((g) => g !== gss)
        : [...prev.exclude, gss],
    }));
  }, []);

  const selectBorough = useCallback(
    (index: number | null) => {
      setState((prev) => ({ ...prev, borough: index === null ? null : boroughs[index].gss }));
    },
    [boroughs],
  );

  const selectedIndex = state.borough
    ? boroughs.findIndex((b) => b.gss === state.borough)
    : -1;

  const row = useMemo(
    () => rowFor(series, state.metric, state.year, boroughs.length),
    [series, state.metric, state.year, boroughs.length],
  );

  // The scale is built from the INCLUDED boroughs only, so excluding City of
  // London genuinely re-classes the remaining 32 rather than just dimming one
  // shape on a scale still stretched to reach it.
  const scale = useMemo(() => {
    const values = row.map((v, i) => (excluded.has(boroughs[i].gss) ? null : v));
    return buildColourScale(metric, values);
  }, [row, excluded, boroughs, metric]);

  const noDataCount = row.filter((v, i) => v === null && !excluded.has(boroughs[i].gss)).length;

  return (
    <div className="space-y-4">
      {(rejected.length > 0 || notice) && (
        <div
          role="status"
          className="rounded-md border border-[var(--status-warning)] bg-[var(--surface-1)] px-3 py-2 text-sm text-[var(--text-primary)]"
        >
          {[...rejected, notice].filter(Boolean).map((m) => (
            <p key={m as string}>{m}</p>
          ))}
        </div>
      )}

      <KpiPanel
        boroughs={boroughs}
        metric={metric}
        metricId={state.metric}
        series={series}
        year={state.year}
        excluded={excluded}
      />

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        {/* Map first in DOM order, so the controls stack below it on mobile :
            issue 3.1's stacking rule : and the reading order matches. */}
        <section
          aria-labelledby="map-heading"
          className="rounded-lg border border-[var(--border)] bg-[var(--surface-1)] p-4 sm:p-5 lg:col-span-2"
        >
          <div className="flex flex-wrap items-baseline justify-between gap-2">
            <h2 id="map-heading" className="text-sm font-semibold text-[var(--text-primary)]">
              {metric.label} by borough
            </h2>
            <span className="text-xs tabular-nums text-[var(--text-secondary)]">
              {describeYear(state.year, metric)}
            </span>
          </div>

          <div className="mt-3">
            <Choropleth
              boroughs={boroughs}
              shapes={shapes}
              values={row}
              scale={scale}
              metric={metric}
              year={state.year}
              excluded={excluded}
              selected={selectedIndex >= 0 ? selectedIndex : null}
              hovered={hovered}
              onSelect={selectBorough}
              onHover={setHovered}
              viewBox={viewBox}
            />
          </div>

          <Legend
            scale={scale}
            metric={metric}
            excludedCount={excluded.size}
            noDataCount={noDataCount}
          />

          <BoroughTable
            boroughs={boroughs}
            values={row}
            metric={metric}
            year={state.year}
            excluded={excluded}
            selected={selectedIndex >= 0 ? selectedIndex : null}
            hovered={hovered}
            onSelect={selectBorough}
            onHover={setHovered}
            onToggleExclude={toggleExclude}
          />
        </section>

        <section
          aria-labelledby="controls-heading"
          className="rounded-lg border border-[var(--border)] bg-[var(--surface-1)] p-4 sm:p-5"
        >
          <h2 id="controls-heading" className="text-sm font-semibold text-[var(--text-primary)]">
            Controls
          </h2>
          <div className="mt-3 space-y-4">
            <MetricControls
              metrics={metrics}
              metric={metric}
              metricId={state.metric}
              boroughs={boroughs}
              excluded={excluded}
              onMetric={selectMetric}
              onToggleExclude={toggleExclude}
              onClearExclusions={() => setState((prev) => ({ ...prev, exclude: [] }))}
            />
            <YearControl
              metric={metric}
              year={state.year}
              onYear={(year) => {
                setNotice(null);
                setState((prev) => ({ ...prev, year }));
              }}
            />
          </div>
        </section>

        <section
          aria-labelledby="scatter-heading"
          className="rounded-lg border border-[var(--border)] bg-[var(--surface-1)] p-4 sm:p-5 lg:col-span-2"
        >
          <h2 id="scatter-heading" className="sr-only">
            Crime against the selected determinant
          </h2>
          <Scatterplot
            boroughs={boroughs}
            metrics={metrics}
            series={series}
            outcomeId="crime_rate_per_1000"
            compareId={state.compare}
            year={state.year}
            excluded={excluded}
            hovered={hovered}
            selected={selectedIndex >= 0 ? selectedIndex : null}
            onHover={setHovered}
            onSelect={selectBorough}
            onCompare={(id) => setState((prev) => ({ ...prev, compare: id }))}
          />
        </section>

        {selectedIndex >= 0 ? (
          <BoroughDetail
            borough={boroughs[selectedIndex]}
            boroughIndex={selectedIndex}
            boroughs={boroughs}
            metrics={metrics}
            series={series}
            year={state.year}
            onClose={() => selectBorough(null)}
          />
        ) : (
          <section
            aria-labelledby="detail-placeholder-heading"
            className="rounded-lg border border-[var(--border)] bg-[var(--surface-1)] p-4 sm:p-5"
          >
            <h2
              id="detail-placeholder-heading"
              className="text-sm font-semibold text-[var(--text-primary)]"
            >
              Borough detail
            </h2>
            <p className="mt-2 text-sm text-[var(--text-secondary)]">
              Select a borough : on the map, in the table, or on the scatterplot : to see all{" "}
              {Object.keys(metrics).length} metrics for it, each ranked against the boroughs that
              metric actually covers.
            </p>
          </section>
        )}
      </div>
    </div>
  );
}
