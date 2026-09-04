"""Property-based tests for the scale-oriented data access layer.

Implements the data-access correctness properties from the design's Correctness
Properties section:

* **Property 10: iter_time_chunks partitions the range exactly** (Task 9.2)
* **Property 11: get_price_page pagination is a lossless partition** (Task 9.6)

Each property runs under the ``ge`` Hypothesis profile (``max_examples>=100``)
registered in ``tests/conftest.py``.
"""

from __future__ import annotations

from unittest.mock import patch

from hypothesis import given
from hypothesis import strategies as st

from ge_pipeline import data_access
from ge_pipeline.data_access import get_price_page, iter_time_chunks

# Unix-second bounds kept in a modest range so generated ranges stay cheap to
# partition while still exercising many-chunk cases.
_TIMESTAMPS = st.integers(min_value=0, max_value=10_000)


@st.composite
def _range_and_chunk(draw: st.DrawFn) -> tuple[int, int, int]:
    """Draw ``(start, stop, chunk_seconds)`` with ``start <= stop`` and chunk > 0."""
    start = draw(_TIMESTAMPS)
    stop = draw(st.integers(min_value=start, max_value=10_000))
    chunk_seconds = draw(st.integers(min_value=1, max_value=1_000))
    return start, stop, chunk_seconds


@given(params=_range_and_chunk())
def test_iter_time_chunks_partitions_range_exactly(
    params: tuple[int, int, int],
) -> None:
    """Chunks form a contiguous, non-overlapping partition of ``[start, stop)``.

    **Property 10: iter_time_chunks partitions the range exactly**

    **Validates: Requirements 10.1, 10.2**
    """
    start, stop, chunk_seconds = params
    chunks = list(iter_time_chunks(start, stop, chunk_seconds))

    # chunks == [] iff start == stop.
    if start == stop:
        assert chunks == []
        return

    assert chunks, "a non-empty range must yield at least one chunk"

    # Each window is half-open, non-empty, and bounded by chunk_seconds.
    for a, b in chunks:
        assert 0 < b - a <= chunk_seconds

    # Consecutive windows are contiguous: chunk[k].stop == chunk[k+1].start.
    for (a_prev, b_prev), (a_next, _b_next) in zip(chunks, chunks[1:]):
        assert b_prev == a_next

    # Exact cover: first starts at start, last stops at stop.
    assert chunks[0][0] == start
    assert chunks[-1][1] == stop


@st.composite
def _dataset_and_limit(draw: st.DrawFn) -> tuple[list[dict], int]:
    """Draw a deterministic ascending-time point set plus a page ``limit``.

    Times are strictly ascending and unique so a point's ``time`` uniquely
    identifies it (matching the cursor contract, which advances by last time).
    """
    n = draw(st.integers(min_value=0, max_value=50))
    # Strictly increasing unique times via cumulative positive gaps.
    gaps = draw(
        st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=n,
            max_size=n,
        )
    )
    times: list[int] = []
    current = draw(st.integers(min_value=0, max_value=1_000))
    for gap in gaps:
        current += gap
        times.append(current)

    points = [
        {"time": t, "avgHighPrice": t * 2, "avgLowPrice": t}
        for t in times
    ]
    limit = draw(st.integers(min_value=1, max_value=max(n, 1)))
    return points, limit


@given(data=_dataset_and_limit())
def test_get_price_page_is_lossless_partition(
    data: tuple[list[dict], int],
) -> None:
    """Following cursors visits every point exactly once, in ascending order.

    **Property 11: get_price_page pagination is a lossless partition**

    **Validates: Requirements 12.1, 12.2, 12.3**
    """
    points, limit = data

    # Inject a deterministic query layer so pagination logic is tested in
    # isolation from InfluxDB. The data_access module calls influx.<fn>.
    # ``unittest.mock.patch`` is used (not the monkeypatch fixture) because a
    # function-scoped fixture is not reset between Hypothesis-generated inputs.
    def _fake_query_price_series(client, item_id, start, stop, interval):
        return list(points)

    item_id = "554"
    collected: list[dict] = []
    cursor: int | None = None
    # Bound iterations defensively so a pagination bug cannot loop forever.
    max_iterations = len(points) + 5
    iterations = 0

    with patch.object(
        data_access.influx,
        "query_price_series",
        _fake_query_price_series,
    ):
        while True:
            iterations += 1
            assert iterations <= max_iterations, "pagination failed to terminate"

            page = get_price_page(
                client=None,
                item_id=item_id,
                start=0,
                stop=10_000,
                interval="5m",
                cursor=cursor,
                limit=limit,
            )

            # Page never exceeds the requested limit.
            assert len(page.points) <= limit
            collected.extend(page.points)

            if page.next_cursor is None:
                break
            # next_cursor is the last returned point's time.
            assert page.next_cursor == page.points[-1]["time"]
            cursor = page.next_cursor

    # Lossless: every point visited exactly once, in original ascending order.
    assert collected == points

    # Strictly ascending time with no gaps or overlaps.
    collected_times = [p["time"] for p in collected]
    assert collected_times == sorted(set(collected_times))
    assert collected_times == [p["time"] for p in points]


# ---------------------------------------------------------------------------
# Property 12: stream_dataset round-trips its rows (Task 9.4)
# ---------------------------------------------------------------------------

import io
import json

import pandas as pd

from ge_pipeline.data_access import stream_dataset

# A small pool of numeric-string item ids. Numeric strings survive a CSV round
# trip as ``str`` once canonicalized, so ``"554"`` compares cleanly regardless
# of whether the decoder inferred an int column.
_ITEM_POOL = ("554", "555", "2", "10")


@st.composite
def _rows_range_and_chunk(
    draw: st.DrawFn,
) -> tuple[list[dict], list[str], int, int, int]:
    """Draw ``(rows, item_ids, start, stop, chunk_seconds)`` for a round-trip.

    ``rows`` have a strictly ascending, unique ``_time`` (built from cumulative
    positive gaps) so a row's time uniquely identifies it, an ``itemID`` drawn
    from a small pool, and two integer value columns. ``[start, stop)`` fully
    covers every row's time so the time-chunked partition sees all rows, and
    ``chunk_seconds`` is kept small to exercise many-window boundary cases.
    """
    n = draw(st.integers(min_value=0, max_value=30))
    gaps = draw(
        st.lists(
            st.integers(min_value=1, max_value=20),
            min_size=n,
            max_size=n,
        )
    )
    current = draw(st.integers(min_value=0, max_value=100))
    times: list[int] = []
    for gap in gaps:
        current += gap
        times.append(current)

    rows: list[dict] = []
    for t in times:
        item = draw(st.sampled_from(_ITEM_POOL))
        rows.append(
            {
                "_time": t,
                "itemID": item,
                "avgHighPrice": draw(st.integers(min_value=0, max_value=10_000)),
                "avgLowPrice": draw(st.integers(min_value=0, max_value=10_000)),
            }
        )

    # Cover [start, stop) around every row so the chunk partition sees them all.
    if times:
        start = draw(st.integers(min_value=0, max_value=times[0]))
        stop = times[-1] + draw(st.integers(min_value=1, max_value=10))
    else:
        start = draw(st.integers(min_value=0, max_value=50))
        stop = start + draw(st.integers(min_value=0, max_value=50))

    # item_ids passed to stream_dataset must be non-empty; the fake query layer
    # partitions purely on time, so the exact list only needs to be valid.
    item_ids = sorted({row["itemID"] for row in rows}) or ["554"]

    chunk_seconds = draw(st.integers(min_value=1, max_value=50))
    return rows, item_ids, start, stop, chunk_seconds


def _canonical(rows: list[dict]) -> list[tuple]:
    """Return rows as ``(time, itemID, avgHighPrice, avgLowPrice)`` tuples.

    Coerces to ``(int, str, int, int)`` so dtype/index noise introduced by a
    format round trip (e.g. CSV inferring an int ``itemID`` column) does not
    cause spurious inequality.
    """
    return [
        (
            int(r["_time"]),
            str(r["itemID"]),
            int(r["avgHighPrice"]),
            int(r["avgLowPrice"]),
        )
        for r in rows
    ]


def _decode(fmt: str, payload: bytes) -> list[dict]:
    """Decode concatenated ``stream_dataset`` bytes back into row dicts."""
    if not payload:
        # Empty datasets stream no bytes in any format.
        return []
    if fmt == "ndjson":
        return [
            json.loads(line)
            for line in payload.decode("utf-8").splitlines()
            if line.strip()
        ]
    if fmt == "csv":
        frame = pd.read_csv(io.BytesIO(payload))
        return frame.to_dict(orient="records")
    # parquet
    frame = pd.read_parquet(io.BytesIO(payload))
    return frame.to_dict(orient="records")


@given(data=_rows_range_and_chunk())
def test_stream_dataset_round_trips_rows(
    data: tuple[list[dict], list[str], int, int, int],
) -> None:
    """Decoding+concatenating the stream reproduces every row, once, in order.

    **Property 12: stream_dataset round-trips its rows**

    **Validates: Requirements 10.4, 11.2, 11.3**
    """
    rows, item_ids, start, stop, chunk_seconds = data

    # Fake query layer that honours the chunking contract: for each requested
    # half-open window it returns exactly the rows whose ``_time`` falls in
    # ``[chunk_start, chunk_stop)``. Every row in [start, stop) therefore lands
    # in exactly one chunk (no loss, no duplication across boundaries).
    def _fake_query_chunk(client, req_item_ids, chunk_start, chunk_stop, interval):
        window = [
            row for row in rows if chunk_start <= row["_time"] < chunk_stop
        ]
        return pd.DataFrame(window, columns=["_time", "itemID", "avgHighPrice", "avgLowPrice"])

    expected = _canonical([r for r in rows if start <= r["_time"] < stop])

    # ``unittest.mock.patch`` (not the monkeypatch fixture) because Hypothesis
    # re-runs this body many times and function-scoped fixtures are not reset
    # between generated examples.
    with patch.object(data_access.influx, "query_chunk", _fake_query_chunk):
        for fmt in ("ndjson", "csv", "parquet"):
            payload = b"".join(
                stream_dataset(
                    client=None,
                    item_ids=item_ids,
                    start=start,
                    stop=stop,
                    interval="5m",
                    fmt=fmt,
                    chunk_seconds=chunk_seconds,
                )
            )
            decoded = _canonical(_decode(fmt, payload))

            # Lossless, duplication-free, and time-ascending across boundaries.
            assert decoded == expected, f"round trip mismatch for fmt={fmt}"
            decoded_times = [row[0] for row in decoded]
            assert decoded_times == sorted(decoded_times)
