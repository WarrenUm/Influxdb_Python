import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ApiError,
  DEFAULT_API_BASE_URL,
  exportDatasetUrl,
  getHealth,
  getOutlierMethods,
  getPricePage,
  getPriceSeries,
  searchItems,
} from "./client";
import type {
  ItemSearchResult,
  PricePage,
  PriceSeriesResponse,
} from "./types";

/**
 * Tests for the typed API client (Requirement 17.6).
 *
 * `fetch` is stubbed so we can assert that each wrapper builds the right
 * URL/query params and parses the JSON body into the transport types defined
 * in `./types.ts`.
 */

/** Build a JSON `Response` the way the global `fetch` mock should resolve. */
function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

/** Install a `fetch` spy resolving to `response` and return the spy. */
function mockFetch(response: Response) {
  return vi.spyOn(globalThis, "fetch").mockResolvedValue(response);
}

describe("api client", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("targets the default base URL for dataset export", () => {
    const url = exportDatasetUrl(["554", "565"], "7d", "1h", "csv");
    expect(url.startsWith(`${DEFAULT_API_BASE_URL}/api/datasets/export`)).toBe(true);
    expect(url).toContain("items=554%2C565");
    expect(url).toContain("range=7d");
    expect(url).toContain("interval=1h");
    expect(url).toContain("format=csv");
  });

  it("parses a typed item-search response and builds the query string", async () => {
    const payload: ItemSearchResult[] = [{ itemId: "554", name: "554" }];
    const fetchMock = mockFetch(jsonResponse(payload));

    const result = await searchItems("55", 10);

    // Parses JSON into the transport type.
    expect(result).toEqual(payload);
    const requestedUrl = fetchMock.mock.calls[0]?.[0] as string;
    expect(requestedUrl).toContain(`${DEFAULT_API_BASE_URL}/api/items`);
    expect(requestedUrl).toContain("query=55");
    expect(requestedUrl).toContain("limit=10");
  });

  it("omits undefined query params (searchItems without a limit)", async () => {
    const fetchMock = mockFetch(jsonResponse([]));

    await searchItems("bow");

    const requestedUrl = fetchMock.mock.calls[0]?.[0] as string;
    expect(requestedUrl).toContain("query=bow");
    expect(requestedUrl).not.toContain("limit=");
  });

  it("builds the price-series URL with path + query and parses the response", async () => {
    const payload: PriceSeriesResponse = {
      itemId: "554",
      interval: "1h",
      points: [
        {
          time: 1_700_000_000,
          avgHighPrice: 5,
          avgLowPrice: 4,
          highPriceVolume: 10,
          lowPriceVolume: 8,
          isOutlier: false,
        },
      ],
    };
    const fetchMock = mockFetch(jsonResponse(payload));

    const result = await getPriceSeries("554", "7d", "1h", "zscore");

    expect(result).toEqual(payload);
    const requestedUrl = fetchMock.mock.calls[0]?.[0] as string;
    expect(requestedUrl).toContain("/api/items/554/prices");
    expect(requestedUrl).toContain("range=7d");
    expect(requestedUrl).toContain("interval=1h");
    expect(requestedUrl).toContain("method=zscore");
  });

  it("url-encodes the item id in the price-series path", async () => {
    const fetchMock = mockFetch(
      jsonResponse({ itemId: "a/b", interval: "raw", points: [] }),
    );

    await getPriceSeries("a/b", "all");

    const requestedUrl = fetchMock.mock.calls[0]?.[0] as string;
    expect(requestedUrl).toContain("/api/items/a%2Fb/prices");
  });

  it("passes the cursor and limit for a price page", async () => {
    const payload: PricePage = {
      itemId: "554",
      interval: "raw",
      points: [],
      nextCursor: null,
    };
    const fetchMock = mockFetch(jsonResponse(payload));

    const result = await getPricePage("554", "7d", 1_700_000_000, 50);

    expect(result).toEqual(payload);
    const requestedUrl = fetchMock.mock.calls[0]?.[0] as string;
    expect(requestedUrl).toContain("/api/items/554/prices/page");
    expect(requestedUrl).toContain("cursor=1700000000");
    expect(requestedUrl).toContain("limit=50");
  });

  it("parses the outlier-methods list", async () => {
    mockFetch(jsonResponse(["zscore", "iqr"]));

    await expect(getOutlierMethods()).resolves.toEqual(["zscore", "iqr"]);
  });

  it("raises ApiError with the parsed detail on a non-2xx response", async () => {
    mockFetch(jsonResponse({ detail: "boom" }, 404));

    await expect(getHealth()).rejects.toMatchObject({
      name: "ApiError",
      status: 404,
      detail: "boom",
    } satisfies Partial<ApiError>);
  });

  it("raises ApiError with an undefined detail when the error body is not JSON", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("not json", { status: 500 }),
    );

    const error = await getHealth().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ApiError);
    expect((error as ApiError).status).toBe(500);
    expect((error as ApiError).detail).toBeUndefined();
  });
});
