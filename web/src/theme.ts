/**
 * Theme application with a safe fallback (Requirement 17.4).
 *
 * The SPA renders using a dark theme by default. The dark theme is expressed as
 * a `data-theme="dark"` attribute on the document's root (`<html>`) element,
 * which the stylesheet keys its dark palette off of. Applying the attribute is
 * wrapped in a try/catch so that if anything goes wrong while enabling the dark
 * theme (a throwing DOM environment, a missing/failed theme stylesheet, etc.)
 * we degrade gracefully to a "default" theme instead of letting the failure
 * bubble up and break rendering.
 *
 * The base stylesheet (`index.css`) is written to be fully usable on its own,
 * so the "default" fallback theme is still legible even without the dark
 * palette overrides.
 */

/** Supported theme identifiers. */
export type Theme = "dark" | "default";

/** The attribute used to select a theme on the root element. */
const THEME_ATTRIBUTE = "data-theme";

/**
 * Set the active theme by writing the theme attribute on `<html>`.
 *
 * @param theme The theme to apply.
 * @throws If the document root is unavailable or the attribute cannot be set.
 */
function setTheme(theme: Theme): void {
  const root = document.documentElement;
  if (!root) {
    throw new Error("Document root element is unavailable");
  }
  root.setAttribute(THEME_ATTRIBUTE, theme);
}

/**
 * Apply the dark theme, falling back to the default theme on any failure.
 *
 * This never throws: if enabling the dark theme fails for any reason the error
 * is caught, a best-effort attempt is made to apply the default theme, and the
 * theme that was ultimately applied is returned. The app therefore always
 * renders, satisfying Requirement 17.4.
 *
 * @returns The theme that was successfully applied.
 */
export function applyThemeWithFallback(): Theme {
  try {
    setTheme("dark");
    return "dark";
  } catch (error) {
    // The dark theme failed to load/apply; degrade to the default theme so the
    // interface still renders rather than failing.
    console.warn(
      "Dark theme failed to load; falling back to the default theme.",
      error,
    );
    try {
      setTheme("default");
    } catch {
      // Even the fallback attribute write failed (e.g. no DOM). The base
      // stylesheet remains in effect, so rendering can still proceed.
    }
    return "default";
  }
}
