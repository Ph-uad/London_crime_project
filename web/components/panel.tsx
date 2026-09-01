import type { ReactNode } from "react";

/**
 * A labelled region of the dashboard grid.
 *
 * The shell ships these empty, named for the issue that fills them. An empty
 * panel that says which component belongs there and what it is waiting for
 * reads as unfinished work; an unlabelled grey box reads as a bug.
 */
export function Panel({
  title,
  issue,
  waitingFor,
  className = "",
  children,
}: {
  title: string;
  issue?: string;
  waitingFor?: string;
  className?: string;
  children?: ReactNode;
}) {
  return (
    <section
      aria-labelledby={`panel-${issue ?? title}`.replace(/[^a-z0-9-]/gi, "-")}
      className={`flex flex-col rounded-lg border border-[var(--border)] bg-[var(--surface-1)] p-4 sm:p-5 ${className}`}
    >
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2
          id={`panel-${issue ?? title}`.replace(/[^a-z0-9-]/gi, "-")}
          className="text-sm font-semibold text-[var(--text-primary)]"
        >
          {title}
        </h2>
        {issue && (
          <span className="text-xs text-[var(--text-secondary)] tabular-nums">
            issue {issue}
          </span>
        )}
      </div>
      {children ?? (
        <p className="mt-3 flex min-h-32 flex-1 items-center justify-center rounded-md border border-dashed border-[var(--border)] p-4 text-center text-sm text-[var(--text-secondary)]">
          {waitingFor ?? "Not built yet."}
        </p>
      )}
    </section>
  );
}
