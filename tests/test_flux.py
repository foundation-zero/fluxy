from datetime import datetime, timedelta, timezone
from textwrap import dedent

import pytest
from fluxy import *


@pytest.fixture
def range_query():
    return pipe(
        from_bucket("bucket"),
        range(
            datetime(2020, 1, 1, tzinfo=timezone.utc),
            datetime(2022, 1, 1, tzinfo=timezone.utc),
        ),
    )


def test_from_bucket():
    assert from_bucket("bucket").to_flux() == 'from(bucket: "bucket")'


def test_from_range():
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)"""
    )
    assert (
        pipe(
            from_bucket("bucket"),
            range(
                datetime(2020, 1, 1, tzinfo=timezone.utc),
                datetime(2022, 1, 1, tzinfo=timezone.utc),
            ),
        ).to_flux()
        == expected
    )


def test_from_range_timedelta():
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: -7d)"""
    )
    assert (
        pipe(
            from_bucket("bucket"),
            range(timedelta(days=-7)),
        ).to_flux()
        == expected
    )


def test_from_range_filter(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> filter(fn: (r) => true)"""
    )
    assert pipe(range_query, filter(lambda r: True)).to_flux() == expected


def test_multiple_filters(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> filter(fn: (r) => true)
                      |> filter(fn: (r) => false)"""
    )
    assert (
        pipe(
            range_query,
            filter(lambda r: True),
            filter(lambda r: False),
        ).to_flux()
        == expected
    )


def test_property_filter(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> filter(fn: (r) => r["topic"] == "test_topic")"""
    )
    assert (
        pipe(
            range_query,
            filter(lambda r: r.topic == "test_topic"),
        ).to_flux()
        == expected
    )


def test_property_filter_subscript(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> filter(fn: (r) => r["topic"] == "test_topic")"""
    )
    assert (
        pipe(
            range_query,
            filter(lambda r: r["topic"] == "test_topic"),
        ).to_flux()
        == expected
    )


def test_or_filter(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> filter(fn: (r) => r["topic"] == "test_topic" or true)"""
    )

    assert (
        pipe(range_query, filter(lambda r: (r.topic == "test_topic") | True)).to_flux()
        == expected
    )


def test_or_filter_expression(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> filter(fn: (r) => r["topic"] == "test_topic" or r["topic"] == "other_test_topic")"""
    )

    assert (
        pipe(
            range_query,
            filter(
                lambda r: (r.topic == "test_topic") | (r.topic == "other_test_topic")
            ),
        ).to_flux()
        == expected
    )


def test_and_filter(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> filter(fn: (r) => r["topic"] == "test_topic" and true)"""
    )

    assert (
        pipe(range_query, filter(lambda r: (r.topic == "test_topic") & True)).to_flux()
        == expected
    )


def test_and_filter_expression(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> filter(fn: (r) => r["topic"] == "test_topic" and r["topic"] == "other_test_topic")"""
    )

    assert (
        pipe(
            range_query,
            filter(
                lambda r: (r.topic == "test_topic") & (r.topic == "other_test_topic")
            ),
        ).to_flux()
        == expected
    )


def test_and_or_filter_combination(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> filter(fn: (r) => r["topic"] == "test_topic" and r["topic"] == "other_test_topic" or r["topic"] == "or_topic")"""
    )

    assert (
        pipe(
            range_query,
            filter(
                lambda r: ((r.topic == "test_topic") & (r.topic == "other_test_topic"))
                | (r.topic == "or_topic")
            ),
        ).to_flux()
        == expected
    )


def test_inequality_filter(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> filter(fn: (r) => r["topic"] != "test_topic")"""
    )
    assert (
        pipe(
            range_query,
            filter(lambda r: r.topic != "test_topic"),
        ).to_flux()
        == expected
    )


def test_filter_conform_single(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> filter(fn: (r) => r["topic"] == "test_topic")"""
    )
    assert (
        pipe(
            range_query,
            filter(conform({"topic": "test_topic"})),
        ).to_flux()
        == expected
    )


def test_filter_conform_multiple(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> filter(fn: (r) => r["topic"] == "test_topic" and r["field"] == "test")"""
    )
    assert (
        pipe(
            range_query,
            filter(conform({"topic": "test_topic", "field": "test"})),
        ).to_flux()
        == expected
    )


def test_filter_any_conform_single(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> filter(fn: (r) => r["topic"] == "test_topic")"""
    )
    assert (
        pipe(
            range_query,
            filter(any(conform, [{"topic": "test_topic"}])),
        ).to_flux()
        == expected
    )


def test_filter_any_conform_multiple(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> filter(fn: (r) => r["topic"] == "test_topic" and r["field"] == "test" or r["topic"] == "other_test_topic")"""
    )
    assert (
        pipe(
            range_query,
            filter(
                any(
                    conform,
                    [
                        {"topic": "test_topic", "field": "test"},
                        {"topic": "other_test_topic"},
                    ],
                )
            ),
        ).to_flux()
        == expected
    )


def test_pivot(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> pivot(rowKey: ["_time"], columnKey: ["topic"], valueColumn: "_value")"""
    )
    assert (
        pipe(
            range_query,
            pivot(["_time"], ["topic"], "_value"),
        ).to_flux()
        == expected
    )


def test_aggregate_window(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> aggregateWindow(every: 60s, fn: last, createEmpty: false)"""
    )

    assert (
        pipe(
            range_query,
            aggregate_window(timedelta(minutes=1), WindowOperation.LAST, False),
        ).to_flux()
    ) == expected


def test_drop(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> drop(columns: ["topic"])"""
    )

    assert (pipe(range_query, drop(["topic"])).to_flux()) == expected


def test_keep(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> keep(columns: ["topic"])"""
    )

    assert (pipe(range_query, keep(["topic"])).to_flux()) == expected


def test_map(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> map(fn: (r) => ({ r with newColumn: r._value * 2 }))"""
    )

    assert (
        pipe(range_query, map("(r) => ({ r with newColumn: r._value * 2 })")).to_flux()
    ) == expected


def test_sum(range_query):
    expected = dedent(
        """\
                      from(bucket: "bucket")
                      |> range(start: 2020-01-01T00:00:00+00:00, stop: 2022-01-01T00:00:00+00:00)
                      |> sum(column: "test")"""
    )

    assert (pipe(range_query, sum("test")).to_flux()) == expected


def test_pipe_a_partial_pipe():
    start = pipe(from_bucket("bucket"))
    total = pipe(start, range(timedelta(minutes=1)))

    expected = dedent(
        """\
                from(bucket: "bucket")
                |> range(start: 60s)"""
    )

    assert total.to_flux() == expected


def test_pipe_a_pipe():
    start = pipe(from_bucket("bucket"), range(timedelta(minutes=1)))
    total = pipe(start)

    expected = dedent(
        """\
                from(bucket: "bucket")
                |> range(start: 60s)"""
    )

    assert total.to_flux() == expected


def test_pipe_a_pipe_with_args():
    start = pipe(from_bucket("bucket"), range(timedelta(minutes=1)))
    total = pipe(
        start,
        filter(lambda r: r.topic == "test_topic"),
    )

    expected = dedent(
        """\
                from(bucket: "bucket")
                |> range(start: 60s)
                |> filter(fn: (r) => r["topic"] == "test_topic")"""
    )

    assert total.to_flux() == expected
