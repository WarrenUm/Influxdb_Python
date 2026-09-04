/**
 * Presentational badge that flags outlier price points (Requirement 17.3).
 *
 * Used alongside {@link PriceChart} to give a textual/visual summary of the
 * outliers detected in the current series. The chart itself marks individual
 * outlier points; this badge surfaces the aggregate count so users can tell at
 * a glance whether anomalies are present. Rendering nothing when there are no
 * outliers keeps the header uncluttered for clean series.
 */

/** Props for {@link OutlierBadge}. */
export interface OutlierBadgeProps {
  /** Number of points flagged as outliers in the current series. */
  count: number;
  /** Optional extra class name for layout composition. */
  className?: string;
}

/**
 * Render a small badge summarizing the number of outliers.
 *
 * Renders `null` when `count <= 0` so callers can drop it in unconditionally.
 *
 * @param props The outlier count and optional class name.
 */
export function OutlierBadge({ count, className }: OutlierBadgeProps): JSX.Element | null {
  if (count <= 0) {
    return null;
  }

  const label = count === 1 ? "1 outlier" : `${count} outliers`;
  const classes = className ? `outlier-badge ${className}` : "outlier-badge";

  return (
    <span
      className={classes}
      role="status"
      aria-label={`${label} detected`}
      title={`${label} detected in this series`}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: "0.35rem",
        padding: "0.15rem 0.55rem",
        borderRadius: "999px",
        background: "#f5a04c22",
        border: "1px solid #f5a04c",
        color: "#f5a04c",
        fontSize: "0.8rem",
        fontWeight: 600,
        lineHeight: 1.4,
      }}
    >
      <span
        aria-hidden="true"
        style={{
          width: "0.5rem",
          height: "0.5rem",
          borderRadius: "50%",
          background: "#f5a04c",
        }}
      />
      {label}
    </span>
  );
}
