/**
 * Data-fetching hook for an outlier-annotated price series (Requirement 17.2).
 *
 * `usePriceSeries` wraps `getPriceSeries` and manages the request lifecycle for
 * a single item: it fetches on mount and refetches whenever any input changes,
 * exposes `{ data, loading, error }`, and guards against stale responses so a
 * slow earlier request can never overwrite a newer one. When `itemId` is
 * `null` the hook performs no request and reports an idle, empty state.
 */
import { useEffect, useState } from "react";

import { ApiError, getPriceSeries } from "../api/client";
import type { PriceSeriesResponse } from "../api/types";

/** The value returned by {@link usePriceSeries}. */
export interface UsePriceSeriesResult {
  /** The fetched series, or `null` while idle, loading the first result, or on error. */
  data: PriceSeriesResponse | null;
  /** `true` while a request for the current inputs is in flight. */
  loading: boolean;
  /** The error from the most recent failed request, or `null`. */
  error: Error | null;
}

/**
 * Fetch and track an outlier-annotated price series for a single item.
 *
 * @param itemId The item identifier to query, or `null` to stay idle.
 * @param range Time range, e.g. "7d" or "all".
 * @param interval Optional downsample interval, e.g. "1h".
 * @param method Optional registered outlier-detection method to apply.
 * @returns The current `{ data, loading, error }` for the given inputs.
 */
export function usePriceSeries(
  itemId: string | null,
  range: string,
  interval?: string,
  method?: string,
): UsePriceSeriesResult {
  const [data, setData] = useState<PriceSeriesResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    // Idle state: no item selected, so clear any previous result.
    if (itemId === null) {
      setData(null);
      setLoading(false);
      setError(null);
      return;
    }

    const controller = new AbortController();
    let active = true;

    setLoading(true);
    setError(null);

    getPriceSeries(itemId, range, interval, method, { signal: controller.signal })
      .then((response) => {
        if (!active) {
          return;
        }
        setData(response);
        setError(null);
        setLoading(false);
      })
      .catch((err: unknown) => {
        // Ignore aborts triggered by a superseded/cleaned-up request.
        if (!active || controller.signal.aborted) {
          return;
        }
        setData(null);
        setError(toError(err));
        setLoading(false);
      });

    // Cleanup: mark this request stale and abort it so its result is ignored.
    return () => {
      active = false;
      controller.abort();
    };
  }, [itemId, range, interval, method]);

  return { data, loading, error };
}

/** Normalize an unknown thrown value into an `Error` instance. */
function toError(err: unknown): Error {
  if (err instanceof ApiError || err instanceof Error) {
    return err;
  }
  return new Error(typeof err === "string" ? err : "Failed to load price series");
}
