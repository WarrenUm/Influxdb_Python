/**
 * Typed fetch wrappers over the FastAPI query service (Requirement 17.6).
 *
 * Every wrapper is typed against the transport interfaces in `./types.ts` so
 * the SPA consumes the API through a single, strongly-typed surface. The base
 * URL is configurable via `import.meta.env.VITE_API_BASE_URL` and defaults to
 * `http://localhost:8000`.
 */

import type {
  ExportFormat,
  ItemSearchResult,
  PricePage,
  PriceSeriesResponse,
} from "./types";

/** Default query-service base URL used when `VITE_API_BASE_URL` is unset. */
export const DEFAULT_API_BASE_URL = "http://localhost:8000";

/**
 * Resolve the configured API base URL, stripping any trailing slash so paths
 * can be concatenated without producing a double slash.
 */
function resolveBaseUrl(): string {
  const configured = import.meta.env?.VITE_API_BASE_URL;
  const base = configured && configured.trim() ? configured.trim() : DEFAULT_API_BASE_URL;
  return base.replace(/\/+$/, "");
}

/** Error thrown when the query service returns a non-2xx response. */
export class ApiError extends Error {
  /** The HTTP status code returned by the service. */
  readonly status: number;
  /** The parsed error detail from the typed error body, when available. */
  readonly detail: string | undefined;

  constructor(status: number, message: string, detail?: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
  }
}

/** Options accepted by the low-level request helpers. */
export interface RequestOptions {
  /** Optional AbortSignal to cancel the in-flight request. */
  signal?: AbortSignal;
}

/** Build an absolute URL for `path` with the given query parameters. */
function buildUrl(path: string, params?: Record<string, string | number | undefined | null>): string {
  const url = new URL(`${resolveBaseUrl()}${path}`);
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null) {
        url.searchParams.set(key, String(value));
      }
    }
  }
  return url.toString();
}

/** Extract a human-readable error detail from a failed response body. */
async function readErrorDetail(response: Response): Promise<string | undefined> {
  try {
    const body = (await response.json()) as { detail?: unknown };
    if (body && typeof body.detail === "string") {
      return body.detail;
    }
  } catch {
    // Body was not JSON or was empty; fall through to undefined.
  }
  return undefined;
}

/** Perform a GET request and parse a typed JSON response. */
async function getJson<T>(
  path: string,
  params?: Record<string, string | number | undefined | null>,
  options?: RequestOptions,
): Promise<T> {
  const response = await fetch(buildUrl(path, params), {
    method: "GET",
    headers: { Accept: "application/json" },
    signal: options?.signal,
  });

  if (!response.ok) {
    const detail = await readErrorDetail(response);
    throw new ApiError(
      response.status,
      `Request to ${path} failed with status ${response.status}`,
      detail,
    );
  }

  return (await response.json()) as T;
}

/** Liveness probe. Resolves to `{ status: "ok" }` when the service is up. */
export function getHealth(options?: RequestOptions): Promise<{ status: string }> {
  return getJson<{ status: string }>("/api/health", undefined, options);
}

/** Return the names of all registered outlier detectors. */
export function getOutlierMethods(options?: RequestOptions): Promise<string[]> {
  return getJson<string[]>("/api/outlier-methods", undefined, options);
}

/**
 * Search stored item ids by case-insensitive substring.
 *
 * @param query Substring to match against item ids; empty matches all.
 * @param limit Maximum number of matches to return.
 */
export function searchItems(
  query: string,
  limit?: number,
  options?: RequestOptions,
): Promise<ItemSearchResult[]> {
  return getJson<ItemSearchResult[]>("/api/items", { query, limit }, options);
}

/**
 * Fetch an outlier-annotated, time-ascending price series for one item.
 *
 * @param itemId The item identifier to query.
 * @param range Time range, e.g. "7d" or "all".
 * @param interval Optional downsample interval, e.g. "1h".
 * @param method Registered outlier-detection method to apply.
 */
export function getPriceSeries(
  itemId: string,
  range: string,
  interval?: string,
  method?: string,
  options?: RequestOptions,
): Promise<PriceSeriesResponse> {
  return getJson<PriceSeriesResponse>(
    `/api/items/${encodeURIComponent(itemId)}/prices`,
    { range, interval, method },
    options,
  );
}

/**
 * Fetch one cursor-paginated page of price points for an item.
 *
 * @param itemId The item identifier to page over.
 * @param range Time range, e.g. "7d" or "all".
 * @param cursor The `time` of the last point from the previous page, or null.
 * @param limit Maximum points per page.
 * @param interval Optional downsample interval, e.g. "1h".
 * @param method Registered outlier-detection method to apply.
 */
export function getPricePage(
  itemId: string,
  range: string,
  cursor?: number | null,
  limit?: number,
  interval?: string,
  method?: string,
  options?: RequestOptions,
): Promise<PricePage> {
  return getJson<PricePage>(
    `/api/items/${encodeURIComponent(itemId)}/prices/page`,
    { range, interval, cursor, limit, method },
    options,
  );
}

/**
 * Fetch only the outlier points for an item (a `PriceSeriesResponse` whose
 * `points` contain only flagged points).
 *
 * @param itemId The item identifier to query.
 * @param range Time range, e.g. "7d" or "all".
 * @param interval Optional downsample interval, e.g. "1h".
 * @param method Registered outlier-detection method to select.
 */
export function getOutliers(
  itemId: string,
  range: string,
  interval?: string,
  method?: string,
  options?: RequestOptions,
): Promise<PriceSeriesResponse> {
  return getJson<PriceSeriesResponse>(
    `/api/items/${encodeURIComponent(itemId)}/outliers`,
    { range, interval, method },
    options,
  );
}

/**
 * Build the absolute URL for the streamed dataset export. Exposed as a URL
 * (rather than a fetch) so callers can hand it to the browser for download.
 *
 * @param items Item identifiers to include in the export.
 * @param range Time range, e.g. "7d" or "all".
 * @param interval Downsample interval applied server-side.
 * @param format One of the supported export formats.
 */
export function exportDatasetUrl(
  items: string[],
  range: string,
  interval: string,
  format: ExportFormat,
): string {
  return buildUrl("/api/datasets/export", {
    items: items.join(","),
    range,
    interval,
    format,
  });
}

/**
 * Fetch the streamed dataset export as a `Blob` (for programmatic download).
 *
 * @param items Item identifiers to include in the export.
 * @param range Time range, e.g. "7d" or "all".
 * @param interval Downsample interval applied server-side.
 * @param format One of the supported export formats.
 */
export async function exportDataset(
  items: string[],
  range: string,
  interval: string,
  format: ExportFormat,
  options?: RequestOptions,
): Promise<Blob> {
  const response = await fetch(
    exportDatasetUrl(items, range, interval, format),
    { method: "GET", signal: options?.signal },
  );
  if (!response.ok) {
    const detail = await readErrorDetail(response);
    throw new ApiError(
      response.status,
      `Dataset export failed with status ${response.status}`,
      detail,
    );
  }
  return response.blob();
}

/**
 * Fetch an ML-ready feature frame as JSON records (one record per time index).
 *
 * @param items Item identifiers to include.
 * @param range Time range, e.g. "7d" or "all".
 * @param interval Downsample interval applied server-side.
 */
export function getFeatures(
  items: string[],
  range: string,
  interval: string,
  options?: RequestOptions,
): Promise<Array<Record<string, number | string | null>>> {
  return getJson<Array<Record<string, number | string | null>>>(
    "/api/datasets/features",
    { items: items.join(","), range, interval },
    options,
  );
}
