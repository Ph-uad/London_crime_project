import type { Metadata } from "next";

import { Panel } from "@/components/panel";
import { FACTS } from "@/lib/site";

export const metadata: Metadata = {
  title: "Insights",
  description:
    "Headline findings on how recorded crime in London associates with social determinants.",
};

export default function InsightsPage() {
  return (
    <div className="mx-auto max-w-[1280px] px-4 py-6 sm:px-6 sm:py-8">
      <h1 className="text-2xl font-semibold tracking-tight sm:text-3xl">Insights</h1>
      <p className="mt-2 max-w-prose text-[var(--text-secondary)]">
        Headline findings across {FACTS.boroughs} boroughs,{" "}
        <span className="tabular-nums">
          {FACTS.analysisStart}–{FACTS.analysisEnd}
        </span>
        . Every claim here will be traceable to the exported dataset, and stated as
        an association rather than a cause.
      </p>

      <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-2">
        <Panel
          title="Headline findings"
          issue="4.1"
          waitingFor="Three or four findings with numbers, written after the frontend can show them."
        />
        <Panel
          title="Pipeline architecture"
          issue="4.1"
          waitingFor="Diagram of the original Spark/Hive pipeline alongside this data.table rebuild."
        />
      </div>
    </div>
  );
}
