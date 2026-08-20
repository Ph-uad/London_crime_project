import path from "node:path";
import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // The app lives in web/ but its data lives in ../data/processed, outside the
  // Next project. Tracing has to start at the monorepo root or the deployment
  // bundle will not include it.
  outputFileTracingRoot: path.join(__dirname, ".."),
  outputFileTracingIncludes: {
    // boroughs.json and coverage.json are imported, so they are traced
    // automatically. london.geojson is read at runtime and must be named.
    "/api/geo": ["../data/processed/london.geojson"],
  },
};

export default nextConfig;
