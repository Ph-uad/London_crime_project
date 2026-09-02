"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { formatWithUnit } from "@/lib/format";
import { NO_DATA_PATTERN_ID, type ColourScale } from "@/lib/scales";
import type { BoroughRef, MetricCoverage } from "@/lib/types";

/**
 * The borough choropleth (plan issue 3.2).
 *
 * The SVG is `role="img"` with a description of what it shows. It is not the
 * keyboard path and does not pretend to be: 33 focusable polygons in geographic
 * order is a tab sequence nobody can hold in their head, and a screen reader
 * cannot see a shape anyway. The borough table beneath it carries the same
 * values, the same selection, and the same no-data states, and is the route for
 * keyboard and assistive technology. That split is deliberate — the picture is
 * for people who can see it, the table is for everyone.
 *
 * Pan and zoom operate on the viewBox rather than on a tile layer, so touch
 * pan/zoom works without a map engine. There is no basemap to zoom into; the
 * gesture exists so a reader can pull apart the small inner-London boroughs,
 * which is what it is needed for at 375 px.
 */

const MIN_ZOOM = 1;
const MAX_ZOOM = 8;

interface View {
  x: number;
  y: number;
  k: number;
}

const HOME: View = { x: 0, y: 0, k: 1 };

export interface ChoroplethProps {
  boroughs: readonly BoroughRef[];
  shapes: readonly string[];
  values: readonly (number | null)[];
  scale: ColourScale;
  metric: MetricCoverage;
  year: number;
  excluded: ReadonlySet<string>;
  selected: number | null;
  hovered: number | null;
  onSelect: (index: number | null) => void;
  onHover: (index: number | null) => void;
  viewBox: { width: number; height: number };
}

export function Choropleth({
  boroughs,
  shapes,
  values,
  scale,
  metric,
  year,
  excluded,
  selected,
  hovered,
  onSelect,
  onHover,
  viewBox,
}: ChoroplethProps) {
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [view, setView] = useState<View>(HOME);
  const [tooltip, setTooltip] = useState<{ index: number; x: number; y: number } | null>(null);

  const pointers = useRef(new Map<number, { x: number; y: number }>());
  const dragged = useRef(false);
  const pinch = useRef<{ distance: number; k: number } | null>(null);

  const width = viewBox.width / view.k;
  const height = viewBox.height / view.k;

  /** Keep the visible window inside the map, so panning cannot lose it. */
  const clamp = useCallback(
    (next: View): View => {
      const w = viewBox.width / next.k;
      const h = viewBox.height / next.k;
      return {
        k: next.k,
        x: Math.min(Math.max(next.x, 0), viewBox.width - w),
        y: Math.min(Math.max(next.y, 0), viewBox.height - h),
      };
    },
    [viewBox.width, viewBox.height],
  );

  const zoomAt = useCallback(
    (factor: number, clientX?: number, clientY?: number) => {
      setView((prev) => {
        const k = Math.min(Math.max(prev.k * factor, MIN_ZOOM), MAX_ZOOM);
        if (k === prev.k) return prev;

        const rect = svgRef.current?.getBoundingClientRect();
        // Zoom about the pointer when there is one, about the centre otherwise,
        // so a pinch keeps the thing under the fingers under the fingers.
        const fx = rect && clientX !== undefined ? (clientX - rect.left) / rect.width : 0.5;
        const fy = rect && clientY !== undefined ? (clientY - rect.top) / rect.height : 0.5;

        const anchorX = prev.x + (viewBox.width / prev.k) * fx;
        const anchorY = prev.y + (viewBox.height / prev.k) * fy;
        return clamp({
          k,
          x: anchorX - (viewBox.width / k) * fx,
          y: anchorY - (viewBox.height / k) * fy,
        });
      });
    },
    [clamp, viewBox.width, viewBox.height],
  );

  /**
   * Wheel zoom is registered non-passively on the element, because React's
   * onWheel is passive and cannot preventDefault — without that the page
   * scrolls behind the map while it zooms.
   */
  useEffect(() => {
    const el = svgRef.current;
    if (!el) return;
    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      zoomAt(e.deltaY < 0 ? 1.15 : 1 / 1.15, e.clientX, e.clientY);
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, [zoomAt]);

  const onPointerDown = (e: React.PointerEvent<SVGSVGElement>) => {
    (e.target as Element).setPointerCapture?.(e.pointerId);
    pointers.current.set(e.pointerId, { x: e.clientX, y: e.clientY });
    dragged.current = false;
    if (pointers.current.size === 2) {
      pinch.current = { distance: spread(pointers.current), k: view.k };
    }
  };

  const onPointerMove = (e: React.PointerEvent<SVGSVGElement>) => {
    const previous = pointers.current.get(e.pointerId);
    if (!previous) return;
    pointers.current.set(e.pointerId, { x: e.clientX, y: e.clientY });

    if (pointers.current.size === 2 && pinch.current) {
      const now = spread(pointers.current);
      if (pinch.current.distance > 0) {
        const target = Math.min(
          Math.max((pinch.current.k * now) / pinch.current.distance, MIN_ZOOM),
          MAX_ZOOM,
        );
        setView((prev) => clamp({ ...prev, k: target }));
        dragged.current = true;
      }
      return;
    }

    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect) return;
    const dx = ((e.clientX - previous.x) / rect.width) * width;
    const dy = ((e.clientY - previous.y) / rect.height) * height;
    if (Math.abs(dx) + Math.abs(dy) > 0.5) dragged.current = true;
    setView((prev) => clamp({ ...prev, x: prev.x - dx, y: prev.y - dy }));
  };

  const endPointer = (e: React.PointerEvent<SVGSVGElement>) => {
    pointers.current.delete(e.pointerId);
    if (pointers.current.size < 2) pinch.current = null;
  };

  const describe = (i: number): string => {
    const v = values[i];
    const name = boroughs[i].name;
    if (excluded.has(boroughs[i].gss)) return `${name} — excluded from the scale`;
    if (v === null) return `${name} — no data`;
    return `${name}: ${formatWithUnit(v, metric)}`;
  };

  const noData = values.filter((v) => v === null).length;

  return (
    <div className="relative">
      <svg
        ref={svgRef}
        role="img"
        aria-label={
          `Choropleth of ${metric.label} across ${boroughs.length} London boroughs in ${year}. ` +
          (noData
            ? `${noData} ${noData === 1 ? "borough has" : "boroughs have"} no data and are drawn hatched. `
            : "") +
          `${scale.note} The table below gives the same values.`
        }
        viewBox={`${view.x} ${view.y} ${width} ${height}`}
        className="h-auto w-full touch-none select-none rounded-md"
        style={{ background: "var(--page-plane)", aspectRatio: `${viewBox.width} / ${viewBox.height}` }}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={endPointer}
        onPointerCancel={endPointer}
        onPointerLeave={(e) => {
          endPointer(e);
          onHover(null);
          setTooltip(null);
        }}
      >
        <defs>
          {/*
            No-data is a hatch as well as a colour. Issue 3.8 requires it to be
            distinguishable without relying on colour alone, and a mid grey
            between a pale ramp step and a dark one is precisely where colour
            alone fails — for a deuteranope and for anyone on a dim screen.
          */}
          <pattern
            id={NO_DATA_PATTERN_ID}
            width="6"
            height="6"
            patternUnits="userSpaceOnUse"
            patternTransform="rotate(45)"
          >
            <rect width="6" height="6" style={{ fill: "var(--no-data)" }} />
            <line
              x1="0"
              y1="0"
              x2="0"
              y2="6"
              style={{ stroke: "var(--no-data-ink)", strokeWidth: 1.6, opacity: 0.55 }}
            />
          </pattern>
        </defs>

        {shapes.map((d, i) => {
          const isExcluded = excluded.has(boroughs[i].gss);
          const isActive = selected === i || hovered === i;
          return (
            <path
              key={boroughs[i].gss}
              d={d}
              fillRule="evenodd"
              style={{
                fill: isExcluded ? "var(--no-data)" : scale.fillOf(values[i]),
                // Excluded boroughs are drawn, faded, rather than removed: a
                // hole in the map is read as missing data, which is a different
                // claim from "the reader took this one out of the scale".
                opacity: isExcluded ? 0.35 : 1,
                stroke: isActive ? "var(--text-primary)" : "var(--surface-1)",
                strokeWidth: isActive ? 2.5 / view.k : 0.8 / view.k,
                strokeLinejoin: "round",
                cursor: "pointer",
                transition: "stroke-width 80ms linear",
              }}
              onPointerEnter={(e) => {
                if (e.pointerType !== "mouse") return;
                onHover(i);
                setTooltip({ index: i, x: e.clientX, y: e.clientY });
              }}
              onPointerMove={(e) => {
                if (e.pointerType !== "mouse" || pointers.current.size) return;
                setTooltip({ index: i, x: e.clientX, y: e.clientY });
              }}
              onClick={() => {
                // A click that ended a pan is not a selection.
                if (dragged.current) return;
                onSelect(selected === i ? null : i);
              }}
            />
          );
        })}
      </svg>

      <MapTooltip tooltip={tooltip} describe={describe} container={svgRef} />

      <div className="pointer-events-none absolute right-2 top-2 flex flex-col gap-1">
        <ZoomButton label="Zoom in" onClick={() => zoomAt(1.4)} disabled={view.k >= MAX_ZOOM}>
          +
        </ZoomButton>
        <ZoomButton label="Zoom out" onClick={() => zoomAt(1 / 1.4)} disabled={view.k <= MIN_ZOOM}>
          −
        </ZoomButton>
        <ZoomButton
          label="Reset the map view"
          onClick={() => setView(HOME)}
          disabled={view.k === 1 && view.x === 0 && view.y === 0}
        >
          ⌂
        </ZoomButton>
      </div>
    </div>
  );
}

function ZoomButton({
  label,
  onClick,
  disabled,
  children,
}: {
  label: string;
  onClick: () => void;
  disabled: boolean;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      aria-label={label}
      className="pointer-events-auto flex h-11 w-11 items-center justify-center rounded-md border border-[var(--border)] bg-[var(--surface-1)] text-lg text-[var(--text-secondary)] disabled:opacity-40"
    >
      <span aria-hidden="true">{children}</span>
    </button>
  );
}

/**
 * Mouse-only, and hidden from assistive technology on purpose: everything it
 * says is in the table below and in the detail panel, both of which are
 * reachable. A tooltip that is also an ARIA live region announces on every
 * pixel of mouse movement.
 */
function MapTooltip({
  tooltip,
  describe,
  container,
}: {
  tooltip: { index: number; x: number; y: number } | null;
  describe: (index: number) => string;
  container: React.RefObject<SVGSVGElement | null>;
}) {
  if (!tooltip) return null;
  const rect = container.current?.getBoundingClientRect();
  if (!rect) return null;

  const left = Math.min(Math.max(tooltip.x - rect.left + 12, 8), rect.width - 8);
  const top = Math.min(Math.max(tooltip.y - rect.top - 8, 8), rect.height - 8);

  return (
    <div
      aria-hidden="true"
      className="pointer-events-none absolute z-10 max-w-56 -translate-y-full rounded-md border border-[var(--border)] bg-[var(--surface-1)] px-2 py-1 text-xs text-[var(--text-primary)] shadow-lg"
      style={{ left, top }}
    >
      {describe(tooltip.index)}
    </div>
  );
}

function spread(pointers: Map<number, { x: number; y: number }>): number {
  const [a, b] = [...pointers.values()];
  if (!a || !b) return 0;
  return Math.hypot(a.x - b.x, a.y - b.y);
}
