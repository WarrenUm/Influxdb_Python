/**
 * Top-level application shell (Requirement 17, task 17.4).
 *
 * Composes the debounced {@link ItemSearch} box with the {@link PriceChart}:
 * selecting an item stores its id in state, {@link usePriceSeries} fetches that
 * item's outlier-annotated series for the chosen range/interval, and the chart
 * renders the result. Loading and error states are surfaced, and the empty
 * state is handled by {@link PriceChart} itself when no item is selected or the
 * series has no points (Requirement 17.5).
 *
 * The dark theme with graceful fallback (Requirement 17.4) is applied once at
 * startup in `main.tsx` via `applyThemeWithFallback`.
 */
import { useState } from "react";

import { ItemSearch } from "./components/ItemSearch";
import { PriceChart } from "./components/PriceChart";
import { usePriceSeries } from "./hooks/usePriceSeries";

/** Time ranges offered in the range selector. */
const RANGES = ["1d", "7d", "30d", "90d"] as const;
/** Downsample intervals offered in the interval selector. */
const INTERVALS = ["5m", "1h", "6h", "1d"] as const;

/**
 * The application root. Renders the search box, range/interval controls, and
 * the price chart for the currently selected item.
 */
export default function App(): JSX.Element {
  const [itemId, setItemId] = useState<string | null>(null);
  // Defaults chosen to satisfy backend range/interval guardrails: a 7-day
  // window downsampled to hourly buckets is a modest, bounded query.
  const [range, setRange] = useState<string>("7d");
  const [interval, setInterval] = useState<string>("1h");

  const { data, loading, error } = usePriceSeries(itemId, range, interval);

  return (
    <main className="app">
      <header className="app__header">
        <h1>OSRS Grand Exchange Dashboard</h1>
        <p>Search for an item to view its price history.</p>
      </header>

      <section className="app__controls">
        <ItemSearch onSelect={setItemId} />

        <div className="app__range-controls">
          <label className="app__control">
            <span>Range</span>
            <select
              value={range}
              onChange={(event) => setRange(event.target.value)}
              aria-label="Time range"
            >
              {RANGES.map((r) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </select>
          </label>

          <label className="app__control">
            <span>Interval</span>
            <select
              value={interval}
              onChange={(event) => setInterval(event.target.value)}
              aria-label="Downsample interval"
            >
              {INTERVALS.map((i) => (
                <option key={i} value={i}>
                  {i}
                </option>
              ))}
            </select>
          </label>
        </div>
      </section>

      <section className="app__chart">
        {itemId === null && (
          <p className="app__status" role="status">
            No item selected. Search and pick an item to begin.
          </p>
        )}

        {itemId !== null && loading && (
          <p className="app__status" role="status">
            Loading price series…
          </p>
        )}

        {itemId !== null && error !== null && (
          <p className="app__status app__status--error" role="alert">
            Failed to load price series: {error.message}
          </p>
        )}

        {/*
          PriceChart handles the empty state itself: passing `null` (idle,
          loading, or error) or an empty series renders an empty-state message
          (Requirement 17.5).
        */}
        {itemId !== null && !loading && error === null && (
          <PriceChart series={data} />
        )}
      </section>
    </main>
  );
}
