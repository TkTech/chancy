"""
Metrics plugin for collecting and sharing metrics across workers.
"""

import asyncio
import datetime
import logging
import math
import time
from collections import OrderedDict
from dataclasses import dataclass, replace
from functools import partial
from typing import Literal, cast
from uuid import UUID, uuid4

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from chancy.job import QueuedJob
from chancy.plugin import Plugin

Resolution = Literal["1min", "5min", "1hour", "1day"]
MetricType = Literal["counter", "gauge", "histogram"]
MetricValue = float | dict[str, float | int]
RESOLUTIONS = {"1min": 60, "5min": 300, "1hour": 3600, "1day": 86400}
DEFAULT_POINTS = {"1min": 60, "5min": 288, "1hour": 168, "1day": 90}
CHUNK_SIZE = 12
log = logging.getLogger(__name__)


def utcnow():
    return datetime.datetime.now(datetime.UTC)


def bucket_time(timestamp: datetime.datetime, seconds: int):
    return datetime.datetime.fromtimestamp(
        math.floor(timestamp.timestamp() / seconds) * seconds, datetime.UTC
    )


@dataclass(frozen=True)
class MetricPoint:
    """
    An observed value for one time bucket.

    :param timestamp: Start of the time bucket.
    :param value: A counter total, gauge value, or histogram statistics.
    :param sampled_at: Time of the most recent observation in this bucket.
    """

    timestamp: datetime.datetime
    value: MetricValue
    sampled_at: datetime.datetime


@dataclass
class Metric:
    """
    A metric's observations and summary for a requested time range.

    :param type: ``counter``, ``gauge`` or ``histogram``.
    :param unit: Unit of the recorded values, such as ``count`` or ``seconds``.
    :param data: Observed data points in chronological order.
    """

    type: MetricType
    unit: str
    data: list[MetricPoint]

    @property
    def aggregation(self):
        """
        How values are summarized: ``sum``, ``last`` or ``summary``.
        """
        return {"counter": "sum", "gauge": "last", "histogram": "summary"}[
            self.type
        ]

    @property
    def sampled_at(self):
        """
        Time of the latest observation, or ``None`` if no data is available.
        """
        return max((point.sampled_at for point in self.data), default=None)

    @property
    def summary(self) -> MetricValue | None:
        """
        Summarize observations in this metric's time range.

        Returns the counter total, most recent gauge value, or histogram
        statistics (``count``, ``sum``, ``avg``, ``min`` and ``max``).
        Returns ``None`` when there are no observations.
        """
        if not self.data:
            return None
        if self.type == "counter":
            return sum(cast(float, point.value) for point in self.data)
        if self.type == "gauge":
            return max(self.data, key=lambda point: point.sampled_at).value
        values = [
            cast(dict[str, float | int], point.value) for point in self.data
        ]
        count = sum(value["count"] for value in values)
        total = sum(value["sum"] for value in values)
        return {
            "count": count,
            "sum": total,
            "avg": total / count,
            "min": min(value["min"] for value in values),
            "max": max(value["max"] for value in values),
        }


@dataclass(frozen=True)
class _Bucket:
    type: MetricType
    unit: str
    count: int
    total: float
    minimum: float
    maximum: float
    value: float
    sampled_at: datetime.datetime


class Metrics(Plugin):
    """
    A plugin that collects and aggregates various metrics from jobs and queues.

    The plugin maintains time-series data for various metrics, with automatic
    aggregation and pruning to provide useful historical data.

    Metrics from all workers can be queried together or filtered by worker.

    .. note::
        This plugin is enabled by default, you only need to provide it in the
        list of plugins to customize its arguments or if ``no_default_plugins``
        is set to ``True``.

    Enable the plugin by adding it to the list of plugins in the Chancy
    constructor:

    .. code-block:: python

        from chancy import Chancy
        from chancy.plugins.metrics import Metrics

        async with Chancy(..., plugins=[Metrics()]) as chancy:
            ...

    Data points are aggregated at different resolutions (1 minute, 5 minutes,
    1 hour, 1 day). Counters total observed increments, gauges show the latest
    value, and histograms summarize observations with a weighted average.

    Retention is based on elapsed time, including periods with no observations.
    Missing observations are not treated as zero.

    **Default Retention Policy:**

    .. list-table::
       :header-rows: 1
       :widths: 20 20 30

       * - Resolution
         - Time Buckets
         - Total Time Period
       * - 1 minute
         - 60
         - 1 hour
       * - 5 minutes
         - 288
         - 1 day (24 hours)
       * - 1 hour
         - 168
         - 1 week (7 days)
       * - 1 day
         - 90
         - 90 days (3 months)

    Job counters count saved job updates, including retries, rather than the
    number of jobs currently in a queue. Job execution times are in seconds
    and measure the final attempt. Metrics become available after the next
    flush, every 60 seconds by default; an abrupt worker exit can lose recent
    observations that have not yet been saved.

    .. note::

        While you can use this plugin to record your own arbitrary metrics,
        it's not designed as a general-purpose monitoring solution. For more
        advanced monitoring and visualization, consider using a dedicated
        monitoring tool like Prometheus or Grafana.
    """

    def __init__(
        self,
        *,
        sync_interval: int = 60,
        max_points_per_resolution: dict[Resolution, int] | None = None,
        maximum_metric_age: int = 86400 * 90,
        collection_interval: int = 30,
        max_buffered_points: int = 10000,
    ):
        """
        Initialize the metrics plugin.

        :param sync_interval: How often to save recorded metrics, in seconds.
                              Defaults to 60 seconds.
        :param max_points_per_resolution: How many time buckets to retain at each
                                          resolution. For example, ``{"5min": 144}``
                                          retains 12 hours of five-minute data.
                                          Unspecified resolutions keep their defaults.
        :param maximum_metric_age: The maximum age of metrics to keep in the
                                   database, in seconds. Defaults to 90 days.
        :param collection_interval: The interval at which to collect table size
                                    metrics, in seconds.
        :param max_buffered_points: Maximum number of data points to hold in memory
                                    across workers sharing this plugin. Defaults to
                                    10,000. New points exceeding this limit are
                                    dropped with a warning.
        """
        super().__init__()
        self.max_points = DEFAULT_POINTS | (max_points_per_resolution or {})
        if (
            set(self.max_points) != set(RESOLUTIONS)
            or any(
                not isinstance(v, int) or v <= 0
                for v in self.max_points.values()
            )
            or min(
                sync_interval,
                maximum_metric_age,
                collection_interval,
                max_buffered_points,
            )
            <= 0
        ):
            raise ValueError(
                "Metric intervals, retention and buffer limits must be positive"
            )
        self.sync_interval = sync_interval
        self.maximum_metric_age = maximum_metric_age
        self.collection_interval = collection_interval
        self.max_buffered_points = max_buffered_points
        self.worker_id = ""
        self.session_id = uuid4()
        self._workers = {}
        self._buckets: dict[
            tuple[str, int, datetime.datetime, UUID, str], _Bucket
        ] = {}
        self._dirty: set[tuple[str, int, datetime.datetime, UUID, str]] = set()
        self._types: dict[str, tuple[MetricType, str]] = {}
        self._flush_lock = asyncio.Lock()
        self._read_lock = asyncio.Lock()
        self._read_cache = OrderedDict()
        self._last_warning = 0.0

    @staticmethod
    def get_identifier():
        return "chancy.metrics"

    def migrate_package(self):
        """
        Get the package that contains the migrations for the metrics plugin.
        """
        return "chancy.plugins.metrics.migrations"

    def migrate_key(self):
        """
        Get the unique identifier for this plugin's migrations.
        """
        return "metrics"

    def get_tables(self):
        """
        Get the names of all tables this plugin is responsible for.
        """
        return ["metrics", "metric_definitions"]

    def api_plugin(self):
        return "chancy.plugins.metrics.api.MetricsApiPlugin"

    async def on_worker_started(self, *, worker):
        if worker in self._workers:
            return
        handler = partial(self._handle_event, worker=worker)
        self._workers[worker] = uuid4(), handler
        for kind in ("counter", "gauge", "histogram"):
            worker.hub.on(f"metrics.{kind}", handler)

    async def on_worker_stopped(self, *, worker):
        # Keep subscriptions alive through final on_job_updated callbacks.
        # A failed flush retains both the subscriptions and dirty snapshots.
        await self.flush(worker.chancy)
        registration = self._workers.pop(worker, None)
        if registration:
            _, handler = registration
            for kind in ("counter", "gauge", "histogram"):
                worker.hub.remove(f"metrics.{kind}", handler)
        self._prune_buffer()

    async def run(self, worker, chancy):
        """
        Run the metrics plugin.

        Periodically save recorded metrics and collect database table sizes.
        Workers start this automatically when the plugin is enabled.
        """
        await self.on_worker_started(worker=worker)
        worker.manager.add(
            "metrics_table_sizes", self._collect_table_sizes(worker, chancy)
        )
        delay = self.sync_interval
        while await self.sleep(delay):
            try:
                await self.flush(chancy)
            except Exception:
                chancy.log.exception(
                    "Metrics flush failed; buffered observations will be retried"
                )
                delay = min(delay * 2, 300)
            else:
                delay = self.sync_interval

    def _warn(self, message):
        # Invalid custom observations must neither flood logs nor stop a worker.
        now = time.monotonic()
        if now - self._last_warning >= 60:
            log.warning(message)
            self._last_warning = now

    def _record(self, key, value, kind, unit, *, worker=None):
        unit = unit or ("count" if kind == "counter" else "number")
        try:
            finite = (
                not isinstance(value, bool)
                and isinstance(value, (int, float))
                and math.isfinite(value)
            )
        except OverflowError:
            finite = False
        try:
            valid_key = (
                isinstance(key, str)
                and "\x00" not in key
                and 0 < len(key.encode()) <= 1024
            )
            valid_unit = (
                isinstance(unit, str)
                and "\x00" not in unit
                and 0 < len(unit) <= 64
            )
            if valid_unit:
                unit.encode()
        except UnicodeEncodeError:
            valid_key = valid_unit = False
        if not valid_key or not valid_unit or not finite:
            self._warn(
                "Dropped invalid metric: keys must be 1–1024 bytes and values finite numbers"
            )
            return
        value = float(value)
        definition = kind, unit
        if self._types.get(key, definition) != definition:
            self._warn(f"Dropped metric with conflicting type or unit: {key}")
            return
        now = utcnow()
        session = self._workers.get(worker)
        session_id = session[0] if session else self.session_id
        worker_id = worker.worker_id if worker is not None else self.worker_id
        keys = [
            (key, seconds, bucket_time(now, seconds), session_id, worker_id)
            for seconds in RESOLUTIONS.values()
        ]
        if (
            len(self._buckets) + sum(k not in self._buckets for k in keys)
            > self.max_buffered_points
        ):
            self._warn(
                "Metrics buffer is full; dropping new buckets until it can be flushed"
            )
            return
        updates = {}
        for k in keys:
            old = self._buckets.get(k)
            if old is None or kind == "gauge":
                point = _Bucket(kind, unit, 1, value, value, value, value, now)
            else:
                total = old.total + value
                if not math.isfinite(total):
                    self._warn(f"Dropped overflowing metric: {key}")
                    return
                point = replace(
                    old,
                    count=old.count + 1,
                    total=total,
                    minimum=min(old.minimum, value),
                    maximum=max(old.maximum, value),
                    value=value,
                    sampled_at=now,
                )
            updates[k] = point
        self._types[key] = definition
        self._buckets.update(updates)
        self._dirty.update(updates)

    async def increment_counter(
        self, metric_key: str, value: float, *, unit: str = "count"
    ):
        """
        Increment a counter metric.

        Counter metrics accumulate values over time periods.

        :param metric_key: The unique key for the metric
        :param value: The value to increment the counter by
        :param unit: Unit of the increments. Defaults to ``count``; use the same
                     unit for every observation of this metric key.
        """
        self._record(metric_key, value, "counter", unit)

    async def record_gauge(
        self, metric_key: str, value: float, *, unit: str = "number"
    ):
        """
        Record a gauge metric which represents a point-in-time value.

        Gauge metrics record the most recent value in each time bucket.

        :param metric_key: The unique key for the metric
        :param value: The value to record
        :param unit: Unit of the value. Defaults to ``number``; use the same
                     unit for every observation of this metric key.
        """
        self._record(metric_key, value, "gauge", unit)

    async def record_histogram_value(
        self, metric_key: str, value: float, *, unit: str = "number"
    ):
        """
        Record a value to a histogram metric.

        Histogram metrics track statistics (min, max, avg, count) for values
        over time periods.

        :param metric_key: The unique key for the metric
        :param value: The value to record
        :param unit: Unit of the observations, such as ``seconds``. Defaults to
                     ``number``; use the same unit for this metric key.
        """
        self._record(metric_key, value, "histogram", unit)

    async def _handle_event(self, event, *, worker=None):
        self._record(
            event.body.get("key"),
            event.body.get("value"),
            event.name.removeprefix("metrics."),
            event.body.get("unit"),
            worker=worker,
        )

    async def on_job_updated(self, *, worker, job: QueuedJob):
        if job.started_at and job.completed_at:
            duration = (job.completed_at - job.started_at).total_seconds()
            for key in (
                f"job:{job.func}:execution_time",
                f"queue:{job.queue}:execution_time",
            ):
                self._record(
                    key, duration, "histogram", "seconds", worker=worker
                )
        for key in (
            f"job:{job.func}:{job.state.value}",
            f"global:status:{job.state.value}",
            f"queue:{job.queue}:throughput",
            f"queue:{job.queue}:{job.state.value}",
        ):
            self._record(key, 1, "counter", "count", worker=worker)

    def retention(self, resolution):
        """
        Get the retention period for a resolution, in seconds.

        The configured bucket count and ``maximum_metric_age`` both limit how
        long observations remain available.
        """
        return min(
            self.max_points[resolution] * RESOLUTIONS[resolution],
            self.maximum_metric_age,
        )

    def _prune_buffer(self):
        now = utcnow()
        retention = {
            seconds: self.retention(name)
            for name, seconds in RESOLUTIONS.items()
        }
        active_sessions = {session for session, _ in self._workers.values()} | {
            self.session_id
        }
        for key in list(self._buckets):
            _, seconds, timestamp, session_id, _ = key
            expired = (
                timestamp.timestamp() + retention[seconds] <= now.timestamp()
            )
            closed = (
                bucket_time(timestamp, seconds * CHUNK_SIZE).timestamp()
                + seconds * CHUNK_SIZE
                <= now.timestamp()
            )
            if expired or (
                (closed or session_id not in active_sessions)
                and key not in self._dirty
            ):
                self._buckets.pop(key)
                self._dirty.discard(key)
        active = {key[0] for key in self._buckets}
        self._types = {
            key: value for key, value in self._types.items() if key in active
        }

    async def flush(self, chancy):
        """
        Save recorded metrics so they can be queried.

        Workers call this periodically and during graceful shutdown. Call it
        explicitly when you need pending observations saved sooner. If saving
        fails, pending observations are kept so this method can be retried.

        :param chancy: The Chancy application instance.
        """
        async with self._flush_lock:
            self._prune_buffer()
            chunks = {
                (
                    key,
                    seconds,
                    bucket_time(timestamp, seconds * CHUNK_SIZE),
                    session_id,
                    worker_id,
                )
                for key, seconds, timestamp, session_id, worker_id in self._dirty
            }
            grouped = {}
            for key, point in self._buckets.items():
                name, seconds, timestamp, session_id, worker_id = key
                chunk = (
                    name,
                    seconds,
                    bucket_time(timestamp, seconds * CHUNK_SIZE),
                    session_id,
                    worker_id,
                )
                if chunk in chunks:
                    grouped.setdefault(chunk, {})[key] = point
            pending = sorted(grouped)
            for offset in range(0, len(pending), 250):
                batch = pending[offset : offset + 250]
                snapshot = {
                    key: value
                    for chunk in batch
                    for key, value in grouped[chunk].items()
                }
                definitions = {
                    key[0]: (value.type, value.unit)
                    for key, value in snapshot.items()
                }
                async with (
                    chancy.pool.connection() as conn,
                    conn.transaction(),
                    conn.cursor(row_factory=dict_row) as cursor,
                ):
                    await cursor.execute(
                        sql.SQL(
                            """
                            INSERT INTO {definitions} (
                                key,
                                metric_type,
                                unit
                            )
                            SELECT
                                key,
                                metric_type,
                                unit
                            FROM jsonb_to_recordset(%s) AS input(
                                key TEXT,
                                metric_type TEXT,
                                unit TEXT
                            )
                            ORDER BY key
                            ON CONFLICT (key) DO NOTHING
                            """
                        ).format(
                            definitions=sql.Identifier(
                                f"{chancy.prefix}metric_definitions"
                            )
                        ),
                        [
                            Jsonb(
                                [
                                    {
                                        "key": key,
                                        "metric_type": kind,
                                        "unit": unit,
                                    }
                                    for key, (kind, unit) in definitions.items()
                                ]
                            )
                        ],
                    )
                    await cursor.execute(
                        sql.SQL(
                            """
                            SELECT *
                            FROM {}
                            WHERE key = ANY(%s)
                            ORDER BY id
                            FOR KEY SHARE
                            """
                        ).format(
                            sql.Identifier(f"{chancy.prefix}metric_definitions")
                        ),
                        [list(definitions)],
                    )
                    stored = await cursor.fetchall()
                    if len(stored) != len(definitions):
                        raise RuntimeError(
                            "Metric catalog changed during flush; retry required"
                        )
                    ids = {}
                    for row in stored:
                        if definitions[row["key"]] == (
                            row["metric_type"],
                            row["unit"],
                        ):
                            ids[row["key"]] = row["id"]
                        else:
                            self._warn(
                                f"Dropped metric conflicting with its stored definition: {row['key']}"
                            )
                    rows = []
                    for name, seconds, chunk, session_id, worker_id in batch:
                        if name not in ids:
                            continue
                        points = sorted(
                            grouped[
                                name, seconds, chunk, session_id, worker_id
                            ].items()
                        )
                        kind = points[0][1].type
                        rows.append(
                            {
                                "metric_id": ids[name],
                                "resolution": seconds,
                                "chunk": chunk,
                                "session_id": session_id,
                                "worker_id": worker_id,
                                "buckets": [key[2] for key, _ in points],
                                "counts": [v.count for _, v in points]
                                if kind == "histogram"
                                else None,
                                "totals": [v.total for _, v in points]
                                if kind != "gauge"
                                else None,
                                "minimums": [v.minimum for _, v in points]
                                if kind == "histogram"
                                else None,
                                "maximums": [v.maximum for _, v in points]
                                if kind == "histogram"
                                else None,
                                "gauges": [v.value for _, v in points]
                                if kind == "gauge"
                                else None,
                                "sampled_at": [v.sampled_at for _, v in points],
                            }
                        )
                    if rows:
                        await cursor.executemany(
                            sql.SQL(
                                """
                                INSERT INTO {metrics} (
                                    metric_id,
                                    resolution,
                                    chunk,
                                    session_id,
                                    worker_id,
                                    buckets,
                                    counts,
                                    totals,
                                    minimums,
                                    maximums,
                                    gauges,
                                    sampled_at
                                )
                                VALUES (
                                    %(metric_id)s,
                                    %(resolution)s,
                                    %(chunk)s,
                                    %(session_id)s,
                                    %(worker_id)s,
                                    %(buckets)s,
                                    %(counts)s,
                                    %(totals)s,
                                    %(minimums)s,
                                    %(maximums)s,
                                    %(gauges)s,
                                    %(sampled_at)s
                                )
                                ON CONFLICT (metric_id, resolution, chunk, session_id)
                                DO UPDATE SET
                                    buckets = EXCLUDED.buckets,
                                    counts = EXCLUDED.counts,
                                    totals = EXCLUDED.totals,
                                    minimums = EXCLUDED.minimums,
                                    maximums = EXCLUDED.maximums,
                                    gauges = EXCLUDED.gauges,
                                    sampled_at = EXCLUDED.sampled_at
                                """
                            ).format(
                                metrics=sql.Identifier(
                                    f"{chancy.prefix}metrics"
                                )
                            ),
                            rows,
                        )
                for key, value in snapshot.items():
                    if self._buckets.get(key) is value:
                        self._dirty.discard(key)
            self._prune_buffer()
            self._read_cache.clear()

    def window(
        self,
        resolution="5min",
        *,
        start=None,
        end=None,
        range_seconds=None,
        limit=None,
    ):
        """
        Get the start and end times for a metric query.

        Times must include a timezone and align to the chosen resolution. The
        start is inclusive and the end is exclusive. By default the range ends
        at the end of the current, potentially incomplete bucket.

        Accepts the same window options as :meth:`get_metrics`.

        :return: A ``(start, end)`` pair.
        :raises ValueError: If the requested window is invalid.
        """
        if resolution not in RESOLUTIONS:
            raise ValueError("resolution must be 1min, 5min, 1hour or 1day")
        seconds = RESOLUTIONS[resolution]
        retention = self.retention(resolution)
        if limit is not None:
            if start is not None or range_seconds is not None:
                raise ValueError("limit cannot be combined with start or range")
            if not 1 <= limit <= self.max_points[resolution]:
                raise ValueError("limit exceeds the retained bucket count")
            range_seconds = limit * seconds
        if end is None:
            end = bucket_time(utcnow(), seconds) + datetime.timedelta(
                seconds=seconds
            )
        if start is None:
            span = (
                range_seconds
                if range_seconds is not None
                else min(86400, retention)
            )
            if not seconds <= span <= retention or span % seconds:
                raise ValueError(
                    "range must be a multiple of resolution within retention"
                )
            start = end - datetime.timedelta(seconds=span)
        elif range_seconds is not None:
            raise ValueError("start cannot be combined with range")
        for value in (start, end):
            if value.tzinfo is None or value != bucket_time(value, seconds):
                raise ValueError(
                    "start and end must be timezone-aware bucket boundaries"
                )
        if not 0 < (end - start).total_seconds() <= retention:
            raise ValueError("requested window exceeds retention")
        if (end - start).total_seconds() / seconds > 1000:
            raise ValueError("requested window exceeds 1000 buckets")
        return start, end

    async def get_metrics(
        self,
        chancy,
        metric_prefix=None,
        worker_id=None,
        *,
        resolution="5min",
        start=None,
        end=None,
        range_seconds=None,
        limit=None,
    ):
        """
        Get metrics matching the given prefix and time range.

        Metrics from all workers are combined unless ``worker_id`` is provided.
        Counters sum observed increments, gauges return the most recent value,
        and histogram averages are weighted by the number of observations.
        Missing data points are omitted rather than filled with zeros.

        For example, to get the default queue's metrics for the last 24 hours:

        .. code-block:: python

            result = await metrics.get_metrics(
                chancy,
                metric_prefix="queue:default",
                resolution="5min",
                range_seconds=86400,
            )
            for key, metric in result["series"].items():
                print(key, metric.summary, metric.unit)

        :param chancy: The Chancy application instance.
        :param metric_prefix: Optional metric key or prefix. ``queue:default``
                              matches that key and its ``:``-separated descendants.
        :param worker_id: Optional worker ID to filter by.
        :param resolution: Size of each time bucket: ``1min``, ``5min``, ``1hour``
                           or ``1day``. Defaults to ``5min``.
        :param start: Optional inclusive start time, with a timezone and aligned
                      to the chosen resolution. Cannot be combined with
                      ``range_seconds`` or ``limit``.
        :param end: Optional exclusive end time, with a timezone and aligned to
                    the chosen resolution. Defaults to the end of the current,
                    potentially incomplete bucket.
        :param range_seconds: Length of the requested time range, in seconds.
                              Must be a multiple of the resolution and within
                              retention. Defaults to 24 hours or the retention
                              period, whichever is shorter.
        :param limit: Alternative way to specify the range as a number of time
                      buckets, including buckets with no observations. Cannot be
                      combined with ``start`` or ``range_seconds``.
        :return: A dictionary containing ``start``, ``end``, ``resolution``,
                 ``generated_at`` and a ``series`` mapping of keys to metrics.
                 Only saved observations are returned; results may be up to ten
                 seconds behind newly saved data.
        :raises ValueError: If the window is invalid, exceeds retention or 1,000
                            time buckets, or matches more than 100 metric keys.
        """
        start, end = self.window(
            resolution,
            start=start,
            end=end,
            range_seconds=range_seconds,
            limit=limit,
        )
        cache_key = metric_prefix, worker_id, resolution, start, end
        async with self._read_lock:
            cached = self._read_cache.get(cache_key)
            if cached and time.monotonic() - cached[0] < 10:
                self._read_cache.move_to_end(cache_key)
                return cached[1]
            result = await self._query_metrics(
                chancy, metric_prefix, worker_id, resolution, start, end
            )
            self._read_cache[cache_key] = time.monotonic(), result
            while (
                len(self._read_cache) > 32
                or sum(
                    len(metric.data)
                    for _, response in self._read_cache.values()
                    for metric in response["series"].values()
                )
                > 50000
            ):
                self._read_cache.popitem(last=False)
            return result

    async def _query_metrics(
        self, chancy, prefix, worker_id, resolution, start, end
    ):
        async with (
            chancy.pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cursor,
        ):
            condition = sql.SQL("TRUE")
            params = {}
            if prefix:
                escaped = (
                    prefix.replace("\\", "\\\\")
                    .replace("%", "\\%")
                    .replace("_", "\\_")
                )
                condition = sql.SQL("key = %(prefix)s OR key LIKE %(children)s")
                params = {"prefix": prefix, "children": escaped + ":%"}
            await cursor.execute(
                sql.SQL(
                    """
                    SELECT *
                    FROM {definitions}
                    WHERE ({condition})
                    ORDER BY key
                    LIMIT 101
                    """
                ).format(
                    definitions=sql.Identifier(
                        f"{chancy.prefix}metric_definitions"
                    ),
                    condition=condition,
                ),
                params,
            )
            definitions = await cursor.fetchall()
            if len(definitions) > 100:
                raise ValueError(
                    "prefix matches more than 100 series; choose a narrower prefix"
                )
            result = {
                row["key"]: Metric(row["metric_type"], row["unit"], [])
                for row in definitions
            }
            if definitions:
                cutoff = bucket_time(
                    utcnow(), RESOLUTIONS[resolution]
                ) + datetime.timedelta(
                    seconds=RESOLUTIONS[resolution] - self.retention(resolution)
                )
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT
                            d.key,
                            d.metric_type,
                            p.bucket,
                            sum(p.count) AS count,
                            sum(p.total) AS total,
                            min(p.minimum) AS minimum,
                            max(p.maximum) AS maximum,
                            (
                                array_agg(
                                    p.value
                                    ORDER BY p.sampled_at DESC, m.session_id DESC
                                ) FILTER (WHERE d.metric_type = 'gauge')
                            )[1] AS value,
                            max(p.sampled_at) AS sampled_at
                        FROM {metrics} m
                        JOIN {definitions} d ON d.id = m.metric_id
                        CROSS JOIN LATERAL unnest(
                            m.buckets,
                            m.counts,
                            m.totals,
                            m.minimums,
                            m.maximums,
                            m.gauges,
                            m.sampled_at
                        ) AS p(
                            bucket,
                            count,
                            total,
                            minimum,
                            maximum,
                            value,
                            sampled_at
                        )
                        WHERE m.metric_id = ANY(%(ids)s)
                            AND m.resolution = %(resolution)s
                            AND m.chunk >= %(chunk_start)s
                            AND m.chunk < %(end)s
                            AND p.bucket >= %(start)s
                            AND p.bucket < %(end)s
                            {worker_filter}
                        GROUP BY d.id, p.bucket
                        ORDER BY d.key, p.bucket
                        """
                    ).format(
                        metrics=sql.Identifier(f"{chancy.prefix}metrics"),
                        definitions=sql.Identifier(
                            f"{chancy.prefix}metric_definitions"
                        ),
                        worker_filter=sql.SQL("AND m.worker_id = %(worker)s")
                        if worker_id
                        else sql.SQL(""),
                    ),
                    {
                        "ids": [row["id"] for row in definitions],
                        "resolution": RESOLUTIONS[resolution],
                        "start": max(start, cutoff),
                        "chunk_start": bucket_time(
                            max(start, cutoff),
                            RESOLUTIONS[resolution] * CHUNK_SIZE,
                        ),
                        "end": end,
                        "worker": worker_id,
                    },
                )
                for row in await cursor.fetchall():
                    kind = row["metric_type"]
                    value = row["total"] if kind == "counter" else row["value"]
                    if kind == "histogram":
                        value = {
                            "count": int(row["count"]),
                            "sum": row["total"],
                            "avg": row["total"] / int(row["count"]),
                            "min": row["minimum"],
                            "max": row["maximum"],
                        }
                    result[row["key"]].data.append(
                        MetricPoint(row["bucket"], value, row["sampled_at"])
                    )
        return {
            "start": start,
            "end": end,
            "resolution": resolution,
            "generated_at": utcnow(),
            "series": result,
        }

    async def list_metrics(self, chancy):
        """
        Get the keys of all available metrics, in alphabetical order.

        :param chancy: The Chancy application instance.
        :return: A list of metric keys.
        """
        async with (
            chancy.pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cursor,
        ):
            await cursor.execute(
                sql.SQL(
                    """
                    SELECT key
                    FROM {}
                    ORDER BY key
                    """
                ).format(sql.Identifier(f"{chancy.prefix}metric_definitions"))
            )
            return [row["key"] for row in await cursor.fetchall()]

    async def cleanup(self, chancy):
        """
        Clean up old metrics data.

        Called automatically by the Pruner plugin, or may be manually invoked.

        Expired data is excluded from queries even before cleanup runs.
        """
        now = utcnow()
        conditions = []
        for name, seconds in RESOLUTIONS.items():
            cutoff = bucket_time(now, seconds) + datetime.timedelta(
                seconds=seconds - self.retention(name)
            )
            conditions.append(
                sql.SQL("(resolution = {} AND chunk < {})").format(
                    sql.Literal(seconds),
                    sql.Literal(bucket_time(cutoff, seconds * CHUNK_SIZE)),
                )
            )
        async with (
            chancy.pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cursor,
        ):
            await cursor.execute(
                sql.SQL(
                    """
                    WITH expired AS (
                        SELECT ctid
                        FROM {metrics}
                        WHERE {conditions}
                        LIMIT 10000
                        FOR UPDATE SKIP LOCKED
                    )
                    DELETE FROM {metrics}
                    WHERE ctid IN (SELECT ctid FROM expired)
                    """
                ).format(
                    metrics=sql.Identifier(f"{chancy.prefix}metrics"),
                    conditions=sql.SQL(" OR ").join(conditions),
                )
            )
            removed = cursor.rowcount
            # Lock catalog entries before checking for orphans again. Writers
            # take a key-share lock before inserting buckets, preventing a
            # pruning transaction from deleting a concurrently reused key.
            await cursor.execute(
                sql.SQL(
                    """
                    SELECT d.id
                    FROM {definitions} d
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {metrics} m
                        WHERE m.metric_id = d.id
                    )
                    ORDER BY d.id
                    LIMIT 1000
                    FOR UPDATE SKIP LOCKED
                    """
                ).format(
                    definitions=sql.Identifier(
                        f"{chancy.prefix}metric_definitions"
                    ),
                    metrics=sql.Identifier(f"{chancy.prefix}metrics"),
                )
            )
            orphan_ids = [row["id"] for row in await cursor.fetchall()]
            if orphan_ids:
                await cursor.execute(
                    sql.SQL(
                        """
                        DELETE FROM {definitions} d
                        WHERE id = ANY(%s)
                            AND NOT EXISTS (
                                SELECT 1
                                FROM {metrics} m
                                WHERE m.metric_id = d.id
                            )
                        """
                    ).format(
                        definitions=sql.Identifier(
                            f"{chancy.prefix}metric_definitions"
                        ),
                        metrics=sql.Identifier(f"{chancy.prefix}metrics"),
                    ),
                    [orphan_ids],
                )
        self._read_cache.clear()
        return removed or None

    async def _collect_table_sizes(self, worker, chancy):
        """
        Collect size metrics for database tables.

        This runs as a separate task and collects table sizes at the configured collection interval,
        but only when this worker is the leader to avoid duplicate metrics.
        """
        while True:
            await asyncio.sleep(self.collection_interval)
            if not worker.is_leader.is_set():
                continue
            tables = {
                "jobs",
                "queues",
                "workers",
                "leader",
                "queue_rate_limits",
            }
            for plugin in chancy.plugins.values():
                tables.update(plugin.get_tables())
            try:
                async with (
                    chancy.pool.connection() as conn,
                    conn.cursor(row_factory=dict_row) as cursor,
                ):
                    await cursor.execute(
                        """
                        SELECT
                            name,
                            pg_total_relation_size(relation) AS total_size_bytes,
                            pg_relation_size(relation) AS table_size_bytes,
                            pg_indexes_size(relation) AS index_size_bytes
                        FROM unnest(%s::text[], %s::regclass[]) AS t(name, relation)
                        """,
                        [
                            sorted(tables),
                            [
                                sql.Identifier(
                                    f"{chancy.prefix}{name}"
                                ).as_string()
                                for name in sorted(tables)
                            ],
                        ],
                    )
                    for row in await cursor.fetchall():
                        for size in (
                            "total_size_bytes",
                            "table_size_bytes",
                            "index_size_bytes",
                        ):
                            self._record(
                                f"table:{row['name']}:{size}",
                                row[size],
                                "gauge",
                                "bytes",
                                worker=worker,
                            )
            except Exception:
                chancy.log.exception("Could not collect table size metrics")
