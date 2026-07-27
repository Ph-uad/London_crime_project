# London Crime & Social Determinants Dashboard

A portfolio repository for rebuilding a London borough-level crime and quality-of-life dashboard with a reproducible R data pipeline and a Next.js frontend.

## Purpose
This project explores how crime rates and social determinants of health intersect across London boroughs. The goal is to build a reproducible data pipeline, document data provenance, and deliver a web interface for comparing crime with income, IMD, wellbeing, and related metrics.

## Current state vs planned
- Completed: LSOA-to-borough lookup workflow, crime raw-file coverage validation, partial analytic pipeline layout.
- In progress: pipeline consolidation, source documentation, and a consistent repository structure.
- Planned: full data aggregation, borough-level JSON export, GeoJSON boundaries, API routes, interactive choropleth frontend, and narrative insights.

## Directory map
- `pipeline/` — canonical R pipeline scripts, QA routines, and documentation.
- `pipeline/experimental/` — exploratory data transformation scripts moved from `data/`.
- `data/raw/` — ignored raw source files.
- `data/processed/` — derived outputs and cleaned datasets.
- `web/` — Next.js frontend scaffold.
- `projects-plan.md` — implementation roadmap and milestone plan.

## How to run
1. Populate `data/raw/` with source files described in `pipeline/SOURCES.md`.
2. From the repository root, run pipeline scripts in order, for example:
   - `Rscript pipeline/00_download.R`
   - `Rscript pipeline/00_LSAOlookup.R`
   - `Rscript pipeline/00_crime_rowcounts.R`
3. Open the web scaffold with:
   - `cd web && npm install && npm run dev`

## Status
- Completed: 00_download.R, 00_LSAOlookup.R, 00_crime_rowcounts.R, placeholder Next.js scaffold.
- In progress: pipeline documentation, canonical source consolidation, and structured frontend wiring.
- Planned: API routes, choropleth map, metric controls, and final portfolio narrative.

## Key references
- Data sources: `pipeline/SOURCES.md`
- Roadmap: `projects-plan.md`
- Frontend scaffold: `web/`
