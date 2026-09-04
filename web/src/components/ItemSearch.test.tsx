import { afterEach, describe, expect, it, vi } from "vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { ItemSearch } from "./ItemSearch";
import * as client from "../api/client";
import type { ItemSearchResult } from "../api/types";

/**
 * Tests for the debounced item-search component (Requirement 17.1).
 *
 * `searchItems` is mocked so we can assert the request is issued only after the
 * debounce delay, that matches render, and that selecting a match invokes
 * `onSelect` with the item id.
 */
describe("ItemSearch", () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("debounces the search request until the delay elapses", async () => {
    vi.useFakeTimers();
    const matches: ItemSearchResult[] = [{ itemId: "554", name: "554" }];
    const searchSpy = vi
      .spyOn(client, "searchItems")
      .mockResolvedValue(matches);

    render(<ItemSearch onSelect={() => {}} debounceMs={300} />);

    // fireEvent is synchronous and safe to use with fake timers.
    fireEvent.change(screen.getByLabelText("Search items"), {
      target: { value: "554" },
    });

    // Not issued immediately after typing.
    expect(searchSpy).not.toHaveBeenCalled();

    // Advance just short of the debounce window: still not issued.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(299);
    });
    expect(searchSpy).not.toHaveBeenCalled();

    // Cross the debounce threshold; now the request fires.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
    });
    expect(searchSpy).toHaveBeenCalledTimes(1);
    expect(searchSpy).toHaveBeenCalledWith(
      "554",
      20,
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
  });

  it("renders matches and invokes onSelect with the item id when clicked", async () => {
    const matches: ItemSearchResult[] = [
      { itemId: "554", name: "Fire rune" },
      { itemId: "565", name: "Blood rune" },
    ];
    vi.spyOn(client, "searchItems").mockResolvedValue(matches);
    const onSelect = vi.fn();

    const user = userEvent.setup();
    render(<ItemSearch onSelect={onSelect} debounceMs={10} />);

    await user.type(screen.getByLabelText("Search items"), "rune");

    // Matches render after the debounce + resolved request.
    const button = await screen.findByRole("button", { name: "Fire rune" });
    expect(
      screen.getByRole("button", { name: "Blood rune" }),
    ).toBeInTheDocument();

    await user.click(button);
    expect(onSelect).toHaveBeenCalledWith("554");
  });

  it("shows an empty state when a non-empty query returns no matches", async () => {
    vi.spyOn(client, "searchItems").mockResolvedValue([]);

    const user = userEvent.setup();
    render(<ItemSearch onSelect={() => {}} debounceMs={10} />);

    await user.type(screen.getByLabelText("Search items"), "zzz");

    await waitFor(() =>
      expect(screen.getByText("No items found.")).toBeInTheDocument(),
    );
  });

  it("does not issue a request for an empty/whitespace query", async () => {
    const searchSpy = vi.spyOn(client, "searchItems").mockResolvedValue([]);

    const user = userEvent.setup();
    render(<ItemSearch onSelect={() => {}} debounceMs={10} />);

    await user.type(screen.getByLabelText("Search items"), "   ");

    // Give the debounce time to settle; still no request for blank input.
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 30));
    });
    expect(searchSpy).not.toHaveBeenCalled();
  });
});
