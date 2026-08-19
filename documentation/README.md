# Documentation

Dated records of significant work on this project: what was done, what was decided, why,
and what it cost or saved. Kept so that a decision taken months ago can be explained
without reconstructing it from git history.

**Naming:** `YYYY-MM-DD-short-slug.md`, dated by when the record was written.

**These are records, not living documents.** They describe the project as it stood on
their date and are not edited afterwards except to correct a factual error — and a
correction is marked as such in the file rather than applied silently. Current state
lives in the files below instead:

| For | See |
|---|---|
| What the project is and where it stands now | [`../README.md`](../README.md) |
| How to run the pipeline, and its outputs | [`../pipeline/README.md`](../pipeline/README.md) |
| Sources, licences and every analytical decision | [`../pipeline/SOURCES.md`](../pipeline/SOURCES.md) |
| The roadmap, issue acceptance criteria and current status | [`../projects-plan.md`](../projects-plan.md) — the single live roadmap |

## Records

| Date | Record | Covers |
|---|---|---|
| 2026-08-19 | [Pipeline review, rebuild and Epic 1 completion](2026-08-19-pipeline-rebuild-record.md) | The 12-issue review, the `data.table` rewrite, and issues 1.4, 1.7, 1.8, 1.9, 1.10 |
| 2026-08-19 | [Plan revision issues (archived)](2026-08-19-plan-revision-issues.md) | The roadmap amendment merged into `projects-plan.md`. Superseded — kept as the record of what was originally specified |

## Related

[`../codebase-review-issues.md`](../codebase-review-issues.md) is the full findings
register from the 12 August review — the detailed evidence behind the summary in the
record above. It sits at the repository root because `../README.md` links to it as the
review that prompted the current pipeline; move it here if you would rather keep all
records in one place.
