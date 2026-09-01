"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";

import { ThemeToggle } from "@/components/theme-toggle";
import { NAV, SITE } from "@/lib/site";

/**
 * Header and primary navigation.
 *
 * At <768px the links collapse behind a disclosure button rather than wrapping
 * or scrolling horizontally. Every interactive target is at least 44px in its
 * smaller dimension, which is the touch-target floor issue 3.8 will audit.
 */
export function SiteHeader() {
  const pathname = usePathname();
  const [open, setOpen] = useState(false);

  const link = (href: string, label: string) => {
    const active = href === "/" ? pathname === "/" : pathname.startsWith(href);
    return (
      <Link
        key={href}
        href={href}
        // aria-current is what tells a screen reader which page it is on. The
        // underline alone would leave that to sighted users only.
        aria-current={active ? "page" : undefined}
        onClick={() => setOpen(false)}
        className={`flex min-h-11 items-center rounded-md px-3 text-sm ${
          active
            ? "font-semibold text-[var(--text-primary)] underline decoration-2 underline-offset-8"
            : "text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
        }`}
      >
        {label}
      </Link>
    );
  };

  return (
    <header className="border-b border-[var(--border)] bg-[var(--surface-1)]">
      <div className="mx-auto flex max-w-[1280px] flex-wrap items-center gap-3 px-4 py-3 sm:px-6">
        <Link
          href="/"
          className="mr-auto flex min-h-11 flex-col justify-center rounded-md pr-2"
        >
          <span className="text-base font-semibold tracking-tight text-[var(--text-primary)]">
            {SITE.short}
          </span>
          <span className="hidden text-xs text-[var(--text-secondary)] sm:block">
            London boroughs · associations, not causes
          </span>
        </Link>

        <nav aria-label="Primary" className="hidden items-center gap-1 md:flex">
          {NAV.map((n) => link(n.href, n.label))}
        </nav>

        <div className="hidden md:block">
          <ThemeToggle />
        </div>

        <button
          type="button"
          className="inline-flex h-11 min-w-11 items-center justify-center rounded-md border border-[var(--border)] px-3 text-sm text-[var(--text-secondary)] md:hidden"
          aria-expanded={open}
          aria-controls="mobile-nav"
          onClick={() => setOpen((v) => !v)}
        >
          <span aria-hidden="true">{open ? "✕" : "☰"}</span>
          {/* Stable name, state on aria-expanded — a name that flips between
              "Open"/"Close" re-announces as a different control. */}
          <span className="sr-only">Menu</span>
        </button>
      </div>

      {open && (
        <div id="mobile-nav" className="border-t border-[var(--border)] md:hidden">
          <nav
            aria-label="Primary"
            className="mx-auto flex max-w-[1280px] flex-col gap-1 px-4 py-2 sm:px-6"
          >
            {NAV.map((n) => link(n.href, n.label))}
            <div className="py-2">
              <ThemeToggle />
            </div>
          </nav>
        </div>
      )}
    </header>
  );
}
