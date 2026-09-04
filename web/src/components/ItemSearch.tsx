/**
 * Debounced item-search box (Requirement 17.1).
 *
 * As the user types, the query is debounced and issued against `/api/items`
 * via the typed `searchItems` client wrapper. Matches are rendered as a
 * selectable list; choosing one invokes `onSelect` so a parent component
 * (App, task 17.4) can react. Loading and empty states are surfaced, and
 * stale/out-of-order responses are ignored using an AbortController plus a
 * per-request guard.
 */
import { useEffect, useRef, useState } from "react";

import { ApiError, searchItems } from "../api/client";
import type { ItemSearchResult } from "../api/types";
import { useDebounce } from "../hooks/useDebounce";

/** Props for {@link ItemSearch}. */
export interface ItemSearchProps {
  /** Invoked with the chosen item id when a user selects a match. */
  onSelect: (itemId: string) => void;
  /** Debounce delay in milliseconds before issuing a request. Defaults to 300. */
  debounceMs?: number;
  /** Maximum number of matches to request. Defaults to 20. */
  limit?: number;
}

/**
 * A controlled search input that debounces typing before querying the item
 * search endpoint and renders the matching items.
 */
export function ItemSearch({
  onSelect,
  debounceMs = 300,
  limit = 20,
}: ItemSearchProps): JSX.Element {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<ItemSearchResult[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const debouncedQuery = useDebounce(query, debounceMs);

  // Monotonically increasing request id used to discard stale responses that
  // resolve out of order after a newer request has already been issued.
  const latestRequestId = useRef(0);

  useEffect(() => {
    const trimmed = debouncedQuery.trim();

    if (trimmed === "") {
      setResults([]);
      setIsLoading(false);
      setError(null);
      return;
    }

    const requestId = ++latestRequestId.current;
    const controller = new AbortController();

    setIsLoading(true);
    setError(null);

    searchItems(trimmed, limit, { signal: controller.signal })
      .then((matches) => {
        if (requestId !== latestRequestId.current) {
          return; // A newer request superseded this one.
        }
        setResults(matches);
        setIsLoading(false);
      })
      .catch((err: unknown) => {
        if (controller.signal.aborted || requestId !== latestRequestId.current) {
          return; // Aborted or stale; ignore.
        }
        const message =
          err instanceof ApiError
            ? err.detail ?? err.message
            : err instanceof Error
              ? err.message
              : "Search failed";
        setError(message);
        setResults([]);
        setIsLoading(false);
      });

    return () => controller.abort();
  }, [debouncedQuery, limit]);

  const trimmedQuery = debouncedQuery.trim();
  const showEmpty =
    !isLoading && error === null && trimmedQuery !== "" && results.length === 0;

  return (
    <div className="item-search">
      <input
        type="search"
        className="item-search__input"
        placeholder="Search items…"
        value={query}
        aria-label="Search items"
        onChange={(event) => setQuery(event.target.value)}
      />

      {isLoading && (
        <p className="item-search__status" role="status">
          Searching…
        </p>
      )}

      {error !== null && (
        <p className="item-search__status item-search__status--error" role="alert">
          {error}
        </p>
      )}

      {showEmpty && (
        <p className="item-search__status" role="status">
          No items found.
        </p>
      )}

      {results.length > 0 && (
        <ul className="item-search__results">
          {results.map((item) => (
            <li key={item.itemId} className="item-search__result">
              <button
                type="button"
                className="item-search__result-button"
                onClick={() => onSelect(item.itemId)}
              >
                {item.name}
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export default ItemSearch;
