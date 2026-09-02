/**
 * Stub for the `server-only` marker package.
 *
 * That package's default export condition throws on import; only Next's
 * "react-server" condition resolves it to a no-op. Vitest does not set that
 * condition, so route handlers that (correctly) mark themselves server-only
 * would fail to load in tests. Aliasing to this empty module keeps the marker
 * meaningful in the build while letting the tests run.
 */
export {};
