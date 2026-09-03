import { ATTRIBUTIONS, FACTS, PARTIAL_COVERAGE } from "@/lib/site";

/**
 * Footer attributions (plan issue 3.1).
 *
 * The publisher list is derived from each metric's `source` in coverage.json,
 * not hand-typed, so it cannot drift from what the pipeline actually cites. The
 * Ordnance Survey / ONS rights line is required by the boundary data's terms and
 * is separate from the OGL statement.
 */
export function SiteFooter() {
  return (
    <footer className="mt-auto border-t border-[var(--border)] bg-[var(--surface-1)]">
      <div className="mx-auto max-w-[1400px] px-4 py-8 sm:px-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)]">
          Data sources &amp; licence
        </h2>

        <ul className="mt-3 space-y-1 text-sm text-[var(--text-secondary)]">
          {ATTRIBUTIONS.map((s) => (
            <li key={s}>{s}</li>
          ))}
        </ul>

        {PARTIAL_COVERAGE.length > 0 && (
          <p className="mt-4 max-w-prose text-sm text-[var(--text-secondary)]">
            <span className="font-semibold text-[var(--text-primary)]">
              Not every metric covers every borough.
            </span>{" "}
            {PARTIAL_COVERAGE.map(
              (g) => `${g.borough} has no ${g.families.join(" or ")} data`,
            ).join("; ")}
            {" : "}
            the resident population is too small for those estimates to be published.
            Affected boroughs are shown as no-data, never as zero.
          </p>
        )}

        <div className="mt-6 space-y-2 border-t border-[var(--border)] pt-4 text-xs text-[var(--text-secondary)]">
          <p>
            Contains public sector information licensed under the{" "}
            <a
              className="underline underline-offset-2 hover:text-[var(--text-secondary)]"
              href="https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
              rel="license noopener noreferrer"
              target="_blank"
            >
              Open Government Licence v3.0
            </a>
            .
          </p>
          <p>
            Borough boundaries contain both Ordnance Survey and ONS Intellectual
            Property Rights.
          </p>
          <p>
            Observational, ecological analysis of {FACTS.boroughs} aggregated borough
            units. It identifies associations; it does not establish causes.
          </p>
          <p>
            <span className="tabular-nums">{FACTS.analysisStart}–{FACTS.analysisEnd}</span>{" "}
            cross-metric analysis window · crime trend to{" "}
            <span className="tabular-nums">{FACTS.trendEnd}</span> · data generated{" "}
            <span className="tabular-nums">{FACTS.generated.slice(0, 10)}</span>
          </p>
        </div>
      </div>
    </footer>
  );
}
