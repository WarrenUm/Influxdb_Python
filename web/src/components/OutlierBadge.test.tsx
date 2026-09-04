import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";

import { OutlierBadge } from "./OutlierBadge";

/**
 * Tests for the outlier-highlighting badge (Requirement 17.3).
 *
 * The badge summarizes the number of outliers in the current series and
 * renders nothing when there are none.
 */
describe("OutlierBadge", () => {
  afterEach(cleanup);

  it("renders a pluralized count when there are multiple outliers", () => {
    render(<OutlierBadge count={3} />);
    expect(screen.getByText("3 outliers")).toBeInTheDocument();
  });

  it("renders the singular label when there is exactly one outlier", () => {
    render(<OutlierBadge count={1} />);
    expect(screen.getByText("1 outlier")).toBeInTheDocument();
  });

  it("renders nothing when the count is zero", () => {
    const { container } = render(<OutlierBadge count={0} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing when the count is negative", () => {
    const { container } = render(<OutlierBadge count={-2} />);
    expect(container).toBeEmptyDOMElement();
  });
});
