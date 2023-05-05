import { useEffect, useState } from "react";

/**
 * Detect user preferred color scheme based on OS/browser settings
 * @returns {'light' | 'dark'}
 */
export function usePreferredColorScheme() {
  const [colorScheme, setColorScheme] = useState("light");

  useEffect(() => {
    // reference: https://blueprintjs.com/docs/#core/typography.dark-theme
    const updateColorScheme = (mediaQueryOrEvent) => setColorScheme(mediaQueryOrEvent.matches ? "dark" : "light");

    const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");
    updateColorScheme(mediaQuery);

    mediaQuery.addEventListener("change", updateColorScheme);
    return () => mediaQuery.removeEventListener("change", updateColorScheme);
  }, []);

  return colorScheme;
}

/**
 * Apply Blueprint design system's recommended theme class name to the body element
 * @param {'light' | 'dark'} colorScheme
 */
export function useBlueprintThemeClassName(colorScheme) {
  useEffect(() => {
    // reference: https://blueprintjs.com/docs/#core/typography.dark-theme
    document.body.classList[colorScheme === "dark" ? "add" : "remove"]("bp4-dark");
  }, [colorScheme]);
}
