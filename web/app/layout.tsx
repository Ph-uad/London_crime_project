import type { Metadata } from "next";

import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { SITE } from "@/lib/site";
import "./globals.css";

export const metadata: Metadata = {
  title: { default: SITE.name, template: `%s · ${SITE.short}` },
  description: SITE.tagline,
};

/**
 * Applies the stored theme before first paint. Without this the page renders in
 * the OS theme and then swaps, which is a visible flash and, for a viewer who
 * chose light on a dark OS, a flash of exactly the thing they opted out of.
 */
// const THEME_INIT = `try{var t=localStorage.getItem('theme');if(t==='dark'||t==='light'){document.documentElement.dataset.theme=t}}catch(e){}`;

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en-GB" className="h-full">
      <head>
        {/* <script dangerouslySetInnerHTML={{ __html: THEME_INIT }} /> */}
      </head>
      <body className="flex min-h-full flex-col bg-[var(--page-plane)] text-[var(--text-primary)] antialiased">
        {/* First tab stop: lets a keyboard user reach the content without
            traversing the whole navigation on every page. */}
        <a
          href="#main"
          className="sr-only focus:not-sr-only focus:absolute focus:left-4 focus:top-4 focus:z-50 focus:rounded-md focus:bg-[var(--surface-1)] focus:px-4 focus:py-3 focus:text-sm focus:text-[var(--text-primary)] focus:shadow-lg"
        >
          Skip to main content
        </a>
        <SiteHeader />
        <main id="main" tabIndex={-1} className="flex-1">
          {children}
        </main>
        <SiteFooter />
      </body>
    </html>
  );
}
