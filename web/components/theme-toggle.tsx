"use client";

import { useCallback, useSyncExternalStore } from "react";

type Theme = "light" | "dark";

/**
 * Dark mode is a choice, not an automatic flip — the palette's dark steps were
 * selected for the dark surface, so the viewer's preference has to beat the OS
 * setting in both directions. The chosen value is stamped on <html> as
 * data-theme, which globals.css scopes on.
 *
 * The theme lives in the DOM and in a media query, both of which are external
 * to React, so it is read with useSyncExternalStore rather than mirrored into
 * component state inside an effect. That avoids the cascading render React 19
 * warns about, and means an OS theme change while the page is open is picked up
 * without a listener of our own.
 */
function subscribe(onChange: () => void) {
  const observer = new MutationObserver(onChange);
  observer.observe(document.documentElement, {
    attributes: true,
    attributeFilter: ["data-theme"],
  });
  const media = window.matchMedia("(prefers-color-scheme: dark)");
  media.addEventListener("change", onChange);
  return () => {
    observer.disconnect();
    media.removeEventListener("change", onChange);
  };
}

function readTheme(): Theme {
  const stamped = document.documentElement.dataset.theme;
  if (stamped === "dark" || stamped === "light") return stamped;
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

/** On the server there is no theme to read; render a neutral label until hydrated. */
function readServerTheme(): Theme | null {
  return null;
}

export function ThemeToggle() {
  const theme = useSyncExternalStore(subscribe, readTheme, readServerTheme);

  const toggle = useCallback(() => {
    const next: Theme = readTheme() === "dark" ? "light" : "dark";
    const root = document.documentElement;
    root.dataset.theme = next;
    try {
      localStorage.setItem("theme", next);
      delete root.dataset.themePersisted;
    } catch {
      // Private browsing, blocked site data, or a storage partition the browser
      // has refused. The toggle still works for this session; only persistence
      // is lost, and that is not worth interrupting the reader over.
      //
      // It IS worth recording. Swallowing this silently made a CI failure
      // undiagnosable: the attribute above was set, so the click looked like it
      // worked and the assertion on it passed — and then the choice vanished on
      // the next page load, three assertions later, reported as a missing
      // attribute with no hint that storage was the cause. A failed write is now
      // a fact on the document, so a test (or a support question) can tell
      // "the theme did not change" from "the theme changed but was not saved".
      root.dataset.themePersisted = "false";
    }
  }, []);

  const isDark = theme === "dark";
  return (
    <button
      type="button"
      onClick={toggle}
      aria-pressed={theme === null ? undefined : isDark}
      className="inline-flex h-11 min-w-11 items-center justify-center gap-2 rounded-md border border-[var(--border)] px-3 text-sm text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
    >
      <span aria-hidden="true">{isDark ? "◐" : "◑"}</span>
      <span>{theme === null ? "Theme" : isDark ? "Dark" : "Light"}</span>
      <span className="sr-only">
        {theme === null ? "" : `— switch to ${isDark ? "light" : "dark"} theme`}
      </span>
    </button>
  );
}
