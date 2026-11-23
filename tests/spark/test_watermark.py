"""Late, out-of-order and duplicate reports through the real silver transformation.

Each `push` is one arrival (one micro-batch). The watermark is max(event_ts seen) - 2 minutes and,
as in Spark, it is applied from the batch after the one that moved it. A 1-minute window is final,
and emitted once, when the watermark passes its end.
"""
import pytest

from streaming.replay import Replay, event, local_spark

D = "2026-11-16T05:"


@pytest.fixture(scope="module")
def spark():
    s = local_spark("test-watermark")
    yield s
    s.stop()


@pytest.fixture
def rp(spark):
    r = Replay(spark, "2 minutes")
    yield r
    r.stop()


def win(rows, cell, minute):
    return [r for r in rows if r["cell_id"] == cell and r["window_start"].strftime("%H:%M") == f"05:{minute}"]


def test_out_of_order_reports_land_in_their_window(rp):
    assert rp.push([event("c1", D + "00:50Z", prb_util_pct=90.0), event("c1", D + "00:10Z", prb_util_pct=30.0),
                    event("c1", D + "00:30Z", prb_util_pct=60.0)]) == []
    out = rp.push([event("c2", D + "03:01Z")])        # watermark -> 05:01:01, past the 05:00 window
    (w,) = win(out, "c1", "00")
    assert w["reports"] == 3 and w["prb_avg"] == pytest.approx(60.0) and w["prb_max"] == 90.0


def test_window_is_not_emitted_before_the_watermark_passes_its_end(rp):
    assert rp.push([event("c1", D + "00:10Z")]) == []
    assert rp.push([event("c2", D + "02:59Z")]) == []  # watermark 05:00:59 < window end 05:01
    assert len(win(rp.push([event("c2", D + "03:00Z")]), "c1", "00")) == 1   # watermark 05:01:00 = end


def test_late_report_inside_the_watermark_is_counted(rp):
    rp.push([event("c1", D + "00:10Z")])
    rp.push([event("c2", D + "01:30Z")])               # c1's next report is now 80 s behind the stream
    rp.push([event("c1", D + "00:40Z")])
    (w,) = win(rp.push([event("c2", D + "03:30Z")]), "c1", "00")
    assert w["reports"] == 2
    assert rp.dropped == 0


def test_report_behind_the_watermark_is_dropped_and_the_window_is_not_reopened(rp):
    rp.push([event("c1", D + "00:10Z")])
    (w,) = win(rp.push([event("c2", D + "03:30Z")]), "c1", "00")   # window closed with 1 report
    assert w["reports"] == 1
    out = rp.push([event("c1", D + "00:50Z")])         # 160 s behind the newest report: too late
    assert win(out, "c1", "00") == [] and rp.dropped >= 1
    assert len(win(rp.rows, "c1", "00")) == 1          # append mode: emitted once, never corrected


def test_retry_with_the_same_event_id_is_counted_once(rp):
    rp.push([event("c1", D + "00:10Z", event_id="r1")])
    rp.push([event("c1", D + "00:10Z", event_id="r1"), event("c1", D + "00:20Z", event_id="r2")])
    (w,) = win(rp.push([event("c2", D + "03:30Z")]), "c1", "00")
    assert w["reports"] == 2


def test_duplicate_arriving_after_its_window_closed_does_not_change_it(rp):
    rp.push([event("c1", D + "00:10Z", event_id="r1")])
    rp.push([event("c2", D + "03:30Z")])
    rp.push([event("c1", D + "00:10Z", event_id="r1")])
    assert [w["reports"] for w in win(rp.rows, "c1", "00")] == [1]


def test_watermark_is_global_so_a_cell_with_a_lagging_clock_loses_its_data(rp):
    """One watermark per query, not per key: a cell whose clock runs 3 minutes behind the rest is
    always behind the watermark, so every one of its reports is dropped."""
    for m in range(5, 15):
        rp.push([event("c_ok", f"{D}{m:02d}:00Z"), event("c_ok", f"{D}{m:02d}:30Z"),
                 event("c_lag", f"{D}{m - 3:02d}:00Z"), event("c_lag", f"{D}{m - 3:02d}:30Z")])
    ok = [r for r in rp.rows if r["cell_id"] == "c_ok"]
    lag = [r for r in rp.rows if r["cell_id"] == "c_lag"]
    assert len(ok) >= 7 and all(r["reports"] == 2 for r in ok)
    # only the first push, before any watermark existed, gets through for the lagging cell
    assert len(lag) <= 1 and rp.dropped >= 16
