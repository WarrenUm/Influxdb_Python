/**
 * TypeScript transport interfaces mirroring the FastAPI query service
 * (see `ge_pipeline/api.py`). These types define the contract the SPA consumes
 * through the typed fetch wrappers in `./client.ts` (Requirement 17.6).
 */

/** A single price point in a series. `time` is a unix seconds timestamp. */
export interface PricePoint {
  /** Unix seconds timestamp of the point. */
  time: number;
  /** Average high price for the window, or `null` for inactive windows. */
  avgHighPrice: number | null;
  /** Average low price for the window, or `null`. */
  avgLowPrice: number | null;
  /** High-price traded volume, or `null`. */
  highPriceVolume: number | null;
  /** Low-price traded volume, or `null`. */
  lowPriceVolume: number | null;
  /** Whether this point was flagged as an outlier. Always present. */
  isOutlier: boolean;
}

/** A full, time-ascending price series for one item. */
export interface PriceSeriesResponse {
  /** The item identifier the points belong to. */
  itemId: string;
  /** The downsample interval used (e.g. "5m", "1h", or "raw"). */
  interval: string;
  /** Time-ascending price points, each carrying an `isOutlier` flag. */
  points: PricePoint[];
}

/** A single item-search match. */
export interface ItemSearchResult {
  /** The matching item identifier. */
  itemId: string;
  /** A human-readable name (currently mirrors `itemId`). */
  name: string;
}

/** One cursor-paginated page of price points. */
export interface PricePage {
  /** The item identifier the points belong to. */
  itemId: string;
  /** The downsample interval used (e.g. "5m", "1h", or "raw"). */
  interval: string;
  /** The time-ascending price points in this page. */
  points: PricePoint[];
  /** The `time` of the last point when more data exists, else `null`. */
  nextCursor: number | null;
}

/** Supported bulk-export encodings for the datasets endpoint. */
export type ExportFormat = "ndjson" | "csv" | "parquet";
