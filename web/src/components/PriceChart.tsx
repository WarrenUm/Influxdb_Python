/**
 * Candlestick + volume chart with outlier highlighting (Requirements 17.2,
 * 17.3, 17.5).
 *
 * Rendering is delegated to Lightweight Charts. A candlestick series shows the
 * price band per window and a histogram series draws a volume overlay on a
 * separate, bottom-anchored price scale. Points flagged with `isOutlier` are
 * marked on the candlestick series so anomalies stand out. When the series has
 * no points, an empty-state message is rendered instead of an empty chart.
 *
 * ## Candle mapping
 * The store holds `avgHighPrice`/`avgLowPrice` (per-window averages), not true
 * OHLC. We therefore synthesize each candle from the available fields:
 *
 * - `high`  = max(avgHighPrice, avgLowPrice)
 * - `low`   = min(avgHighPrice, avgLowPrice)
 * - `open`  = avgLowPrice  (the "buy"/instant-sell side opens the band)
 * - `close` = avgHighPrice (the "sell"/instant-buy side closes the band)
 *
 * When only one of the two prices is present, both open/close collapse to that
 * value (a doji-like candle). Points with no price data at all are skipped for
 * the candlestick series. This mapping is a visualization convenience, not a
 * claim about intrabar order; it renders the high/low spread each window.
 *
 * Volume overlay uses `highPriceVolume + lowPriceVolume` (null-safe, missing
 * treated as 0). Times are unix seconds, which Lightweight Charts consumes
 * directly as `UTCTimestamp`.
 */
import { useEffect, useMemo, useRef } from "react";
import {
  ColorType,
  createChart,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  type SeriesMarker,
  type UTCTimestamp,
} from "lightweight-charts";

import type { PricePoint, PriceSeriesResponse } from "../api/types";
import { OutlierBadge } from "./OutlierBadge";

/** Props for {@link PriceChart}. */
export interface PriceChartProps {
  /** The series to render, or `null` while no data is available. */
  series: PriceSeriesResponse | null;
  /** Optional fixed chart height in pixels (defaults to 420). */
  height?: number;
}

/** Up (bullish) candle / positive accent color. */
const UP_COLOR = "#26a69a";
/** Down (bearish) candle / negative accent color. */
const DOWN_COLOR = "#ef5350";
/** Outlier marker accent color (matches OutlierBadge). */
const OUTLIER_COLOR = "#f5a04c";

/**
 * Convert one API point into a candlestick datum, or `null` when the point has
 * no usable price data. See the module docstring for the mapping rationale.
 */
function toCandle(point: PricePoint): CandlestickData<UTCTimestamp> | null {
  const { avgHighPrice, avgLowPrice } = point;
  const hasHigh = avgHighPrice !== null;
  const hasLow = avgLowPrice !== null;

  if (!hasHigh && !hasLow) {
    return null;
  }

  // Collapse to whichever value is present when one side is missing.
  const highVal = hasHigh ? avgHighPrice : (avgLowPrice as number);
  const lowVal = hasLow ? avgLowPrice : (avgHighPrice as number);

  const open = lowVal;
  const close = highVal;
  const high = Math.max(highVal, lowVal);
  const low = Math.min(highVal, lowVal);

  return {
    time: point.time as UTCTimestamp,
    open,
    high,
    low,
    close,
  };
}

/** Null-safe combined traded volume for a point. */
function toVolume(point: PricePoint): number {
  return (point.highPriceVolume ?? 0) + (point.lowPriceVolume ?? 0);
}

/**
 * Render a candlestick + volume chart for a price series.
 *
 * @param props The series to render and optional chart height.
 */
export function PriceChart({ series, height = 420 }: PriceChartProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);

  const points = series?.points ?? [];
  const hasData = points.length > 0;
  const outlierCount = useMemo(
    () => points.reduce((acc, p) => (p.isOutlier ? acc + 1 : acc), 0),
    [points],
  );

  // Create the chart once (only while there is data to show) and tear it down
  // on unmount or when the chart is hidden by the empty state.
  useEffect(() => {
    const container = containerRef.current;
    if (!container || !hasData) {
      return;
    }

    const chart = createChart(container, {
      height,
      width: container.clientWidth,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#e6e6e6",
      },
      grid: {
        vertLines: { color: "#232734" },
        horzLines: { color: "#232734" },
      },
      timeScale: { timeVisible: true, secondsVisible: false },
      rightPriceScale: { borderColor: "#232734" },
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: UP_COLOR,
      downColor: DOWN_COLOR,
      borderUpColor: UP_COLOR,
      borderDownColor: DOWN_COLOR,
      wickUpColor: UP_COLOR,
      wickDownColor: DOWN_COLOR,
    });

    // Volume histogram on its own overlay scale, anchored to the bottom.
    const volumeSeries = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "volume",
      color: "#4c8bf5",
    });
    chart.priceScale("volume").applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volumeSeriesRef.current = volumeSeries;

    // Keep the chart width in sync with its container.
    const resize = (): void => {
      if (containerRef.current) {
        chart.resize(containerRef.current.clientWidth, height);
      }
    };
    const observer =
      typeof ResizeObserver !== "undefined" ? new ResizeObserver(resize) : null;
    observer?.observe(container);

    return () => {
      observer?.disconnect();
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      volumeSeriesRef.current = null;
    };
  }, [hasData, height]);

  // Push data into the series whenever the points change.
  useEffect(() => {
    const candleSeries = candleSeriesRef.current;
    const volumeSeries = volumeSeriesRef.current;
    const chart = chartRef.current;
    if (!candleSeries || !volumeSeries || !chart || !hasData) {
      return;
    }

    const candles: CandlestickData<UTCTimestamp>[] = [];
    const volumes: HistogramData<UTCTimestamp>[] = [];
    const markers: SeriesMarker<UTCTimestamp>[] = [];

    for (const point of points) {
      const candle = toCandle(point);
      if (candle === null) {
        continue;
      }
      candles.push(candle);
      volumes.push({
        time: point.time as UTCTimestamp,
        value: toVolume(point),
        color: candle.close >= candle.open ? `${UP_COLOR}99` : `${DOWN_COLOR}99`,
      });
      if (point.isOutlier) {
        markers.push({
          time: point.time as UTCTimestamp,
          position: "aboveBar",
          color: OUTLIER_COLOR,
          shape: "circle",
          text: "outlier",
        });
      }
    }

    candleSeries.setData(candles);
    volumeSeries.setData(volumes);
    candleSeries.setMarkers(markers);
    chart.timeScale().fitContent();
  }, [points, hasData]);

  if (!hasData) {
    return (
      <div className="price-chart price-chart--empty" role="status">
        <p>No price data available for this item and range.</p>
      </div>
    );
  }

  return (
    <div className="price-chart">
      <div className="price-chart__header" style={{ marginBottom: "0.5rem" }}>
        <OutlierBadge count={outlierCount} />
      </div>
      <div
        ref={containerRef}
        className="price-chart__canvas"
        style={{ width: "100%", height }}
      />
    </div>
  );
}
