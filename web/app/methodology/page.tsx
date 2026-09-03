import type { Metadata } from "next";

import { coverage } from "@/lib/data";
import { FACTS } from "@/lib/site";

export const metadata: Metadata = {
  title: "Methodology",
  description:
    "Sources, coverage, analytical decisions and limitations behind the London crime and social determinants dashboard.",
};

/**
 * Methodology and limitations.
 *
 * The coverage table is generated from coverage.json rather than written out, so
 * it cannot claim a year range or a borough count the data does not have. The
 * limitations prose is deliberately hand-written : it is an argument, not a
 * field.
 */
export default function MethodologyPage() {
  const rows = Object.entries(coverage.metrics)
    .map(([id, m]) => ({
      id,
      label: m.label,
      years: m.years.length ? `${Math.min(...m.years)}–${Math.max(...m.years)}` : ":",
      cadence: m.cadence,
      direction: m.direction,
      boroughs: m.boroughs_covered,
    }))
    .sort((a, b) => a.label.localeCompare(b.label));

  return (
    <div className="mx-auto max-w-[1280px] px-4 py-6 sm:px-6 sm:py-8">
      <h1 className="text-2xl font-semibold tracking-tight sm:text-3xl">Methodology</h1>
      <p className="mt-2 max-w-prose text-[var(--text-secondary)]">
        Every figure on this site is regenerated from documented raw sources by an
        ordered R pipeline. Sources, licences and the reasoning behind each
        analytical choice are recorded in the repository&rsquo;s{" "}
        <code className="text-[var(--text-primary)]">pipeline/SOURCES.md</code>.
      </p>

      <h2 className="mt-8 text-lg font-semibold">What is measured, and when</h2>
      {/* A horizontally scrollable region needs to be focusable, or a keyboard
          user cannot scroll it to reach the columns off-screen at 375px. The
          label is what makes it a navigable landmark rather than a stray tab stop. */}
      <div
        tabIndex={0}
        role="region"
        aria-label="Coverage by metric"
        className="mt-3 overflow-x-auto rounded-lg border border-[var(--border)] bg-[var(--surface-1)]"
      >
        <table className="w-full min-w-[42rem] text-left text-sm">
          <caption className="sr-only">
            Coverage by metric: year range, cadence, direction and borough count
          </caption>
          <thead className="border-b border-[var(--border)] text-[var(--text-secondary)]">
            <tr>
              <th scope="col" className="px-4 py-2 font-medium">Metric</th>
              <th scope="col" className="px-4 py-2 font-medium">Years</th>
              <th scope="col" className="px-4 py-2 font-medium">Cadence</th>
              <th scope="col" className="px-4 py-2 font-medium">Higher means</th>
              <th scope="col" className="px-4 py-2 font-medium">Boroughs</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id} className="border-b border-[var(--border)] last:border-0">
                <th scope="row" className="px-4 py-2 font-normal text-[var(--text-primary)]">
                  {r.label}
                </th>
                <td className="px-4 py-2 tabular-nums text-[var(--text-secondary)]">{r.years}</td>
                <td className="px-4 py-2 text-[var(--text-secondary)]">{r.cadence}</td>
                <td className="px-4 py-2 text-[var(--text-secondary)]">
                  {r.direction === "higher_is_worse"
                    ? "worse"
                    : r.direction === "higher_is_better"
                      ? "better"
                      : "neither"}
                </td>
                <td className="px-4 py-2 tabular-nums text-[var(--text-secondary)]">
                  {r.boroughs}
                  {r.boroughs < FACTS.boroughs && (
                    <span className="ml-1 text-[var(--text-secondary)]">
                      of {FACTS.boroughs}
                    </span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <h2 className="mt-8 text-lg font-semibold">Limitations</h2>
      <ul className="mt-3 max-w-prose list-disc space-y-2 pl-5 text-[var(--text-secondary)]">
        <li>
          <strong className="text-[var(--text-primary)]">Associations, not causes.</strong>{" "}
          This is an observational analysis of {FACTS.boroughs} aggregated units. A
          borough-level relationship does not carry down to the people in it.
        </li>
        <li>
          <strong className="text-[var(--text-primary)]">
            The IMD crime domain is excluded.
          </strong>{" "}
          It is built from recorded crime, so using it to explain crime rates would be
          circular. It is kept only as an external check on our own rates.
        </li>
        <li>
          <strong className="text-[var(--text-primary)]">
            Crime categories change in April 2013.
          </strong>{" "}
          Both police.uk vocabularies are mapped into one series, which makes it
          continuous but not comparable across that boundary.
        </li>
        <li>
          <strong className="text-[var(--text-primary)]">
            Two metrics cover 32 boroughs.
          </strong>{" "}
          City of London has too few residents for ONS to publish well-being or life
          expectancy. It is shown as no-data, never as zero.
        </li>
        <li>
          <strong className="text-[var(--text-primary)]">Year conventions differ.</strong>{" "}
          Financial years map to their start year; rolling three-year periods map to
          their end year. Any cross-metric comparison states which years it paired.
        </li>
        <li>
          <strong className="text-[var(--text-primary)]">
            Ungeocoded crime is excluded, not imputed.
          </strong>{" "}
          Records with no LSOA code are counted and set aside. British Transport
          Police records are out of scope.
        </li>
      </ul>
    </div>
  );
}
