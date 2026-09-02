import path from "node:path";

import { defineConfig } from "vitest/config";

/**
 * The route handlers are plain functions over the Web Request/Response API, so
 * they run under Node directly — no Next server, no HTTP listener. Aliases
 * mirror tsconfig paths so tests import exactly what the routes do.
 */
export default defineConfig({
  resolve: {
    alias: {
      "@data": path.resolve(__dirname, "../data/processed"),
      "@": path.resolve(__dirname, "."),
      // `server-only` is a marker package whose default entry throws on import;
      // only Next's "react-server" condition resolves it to a no-op. Vitest does
      // not set that condition, so point it at a local stub. Aliasing here
      // rather than enabling the condition globally keeps React resolving
      // normally, and keeps the marker meaningful in the real build.
      "server-only": path.resolve(__dirname, "tests/stubs/server-only.ts"),
    },
  },
  test: {
    environment: "node",
    include: ["tests/**/*.test.ts"],
  },
});
