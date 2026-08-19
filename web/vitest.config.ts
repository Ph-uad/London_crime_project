import path from "node:path";
import { defineConfig } from "vitest/config";

// The route handlers are plain functions over the Web Request/Response API, so
// they run under Node directly — no Next server, no HTTP listener. Aliases mirror
// tsconfig paths so tests import exactly what the routes do.
export default defineConfig({
  resolve: {
    alias: {
      "@data": path.resolve(__dirname, "../data/processed"),
      "@": path.resolve(__dirname, "."),
    },
  },
  test: {
    environment: "node",
    include: ["tests/**/*.test.ts"],
  },
});
