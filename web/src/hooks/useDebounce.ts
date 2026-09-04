/**
 * Debounce a rapidly-changing value.
 *
 * Returns a copy of `value` that only updates after `delayMs` has elapsed
 * without further changes. Useful for throttling side effects (such as
 * search requests) driven by fast user input (Requirement 17.1).
 */
import { useEffect, useState } from "react";

/**
 * Return a debounced copy of `value` that lags behind by `delayMs`.
 *
 * @param value The value to debounce.
 * @param delayMs Milliseconds of quiet time before the value settles.
 */
export function useDebounce<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState<T>(value);

  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delayMs);
    return () => clearTimeout(timer);
  }, [value, delayMs]);

  return debounced;
}
