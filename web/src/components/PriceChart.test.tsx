import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";

import { PriceChart } from "./PriceChart";
import type { PriceSeriesResponse } from "../api/types";

/**
 * Tests for the price-chart empty-state rendering (Requirement 17.5).
 *
 * The empty-state branch returns before `createChart` is ever called, so it is
 * safe to exercise under jsdom (which has no canvas). We cover both a null
 * series and a series with an empty `points` array.
 */
describe("PriceChart empty state", () => {
  afterEach(cleanup);

  const EMPTY_MESSAGE = "No price data available for this item and range.";

  it("renders the empty-state message when the series is null", () => {
    const { container } = render(<PriceChart series={null} />);
    expect(screen.getByText(EMPTY_MESSAGE)).toBeInTheDocument();
    expect(container.querySelector(".price-chart--empty")).not.toBeNull();
  });

  it("renders the empty-state message when the series has no points", () => {
    const series: PriceSeriesResponse = {
      itemId: "554",
      interval: "raw",
      points: [],
    };
    const { container } = render(<PriceChart series={series} />);
    expect(screen.getByText(EMPTY_MESSAGE)).toBeInTheDocument();
    expect(container.querySelector(".price-chart--empty")).not.toBeNull();
  });
});
