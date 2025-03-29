"""
Metrics plugin for collecting and sharing metrics across workers.
"""

import asyncio
import datetime
import json
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import groupby
from typing import Dict, List, Literal, Optional, Set, Tuple, Union, cast

from psycopg import sql
from psycopg.rows import dict_row

from chancy.app import Chancy
from chancy.job import QueuedJob
from chancy.plugin import Plugin
from chancy.worker import Worker

Resolution = Literal["1min", "5min", "1hour", "1day"]
MetricValue = Union[int, float, Dict[str, Union[int, float]]]
MetricPoint = Tuple[datetime.datetime, MetricValue]
MetricType = Literal["counter", "gauge", "histogram"]


@dataclass
class Metric:
    """
    A class representing a metric with values at different resolutions.
    """

    metric_type: MetricType
    values_1min: List[MetricPoint] = field(default_factory=list)
    values_5min: List[MetricPoint] = field(default_factory=list)
    values_1hour: List[MetricPoint] = field(default_factory=list)
    values_1day: List[MetricPoint] = field(default_factory=list)

    @property
    def values(self) -> Dict[Resolution, List[MetricPoint]]:
        """Get all values in a dictionary keyed by resolution."""
        return {
            "1min": self.values_1min,
            "5min": self.values_5min,
            "1hour": self.values_1hour,
            "1day": self.values_1day,
        }


class Metrics(Plugin):
    """
    A plugin that collects and aggregates various metrics from jobs, queues
    workers, table sizes, and more.

    The plugin maintains time-series data for various metrics, with automatic
    aggregation and pruning to keep storage requirements low while providing
    useful historical data.

    Metrics are periodically synchronized across workers, so each worker has
    access to the full set of metrics, based on the ``sync_interval`` setting.

    Example:

    .. code-block:: python

        from chancy import Chancy
        from chancy.plugins.metrics import Metrics

        async with Chancy(..., plugins=[Metrics()]) as chancy:
            ...

    The metrics are stored in a compact time-series format, with data points
    aggregated at different resolutions (1 minute, 5 minutes, 1 hour, 1 day).

    .. note::

        While you can use this plugin to record your own arbitrary metrics,
        it's not designed as a general-purpose monitoring solution. For more
        advanced monitoring and visualization, consider using a dedicated
        monitoring tool like Prometheus or statsd.
    """

    def __init__(
        self,
        *,
        sync_interval: int = 60,
        max_points_per_resolution: Dict[Resolution, int] = None,
        maximum_metric_age: int = 60 * 60 * 24 * 90,
        collection_interval: int = 30,
    ):
        """
        Initialize the metrics plugin.

        :param sync_interval: How often to synchronize metrics with the
                              database and other workers.
        :param max_points_per_resolution: How many points to keep for each
                                          resolution.
        :param maximum_metric_age: The maximum age of metrics to keep in the
                                   database, in seconds. Defaults to 90 days.
        :param collection_interval: The interval at which to collect table size
                                    metrics, in seconds.
        """
        super().__init__()
        self.sync_interval = sync_interval

        self.worker_id = None

        # Default retention policy: how many points to keep for each resolution
        self.max_points: Dict[Resolution, int] = {
            "1min": 60,  # 1 hour of 1-minute data
            "5min": 288,  # 1 day of 5-minute data
            "1hour": 168,  # 1 week of hourly data
            "1day": 90,  # 90 days of daily data
        }

        if max_points_per_resolution:
            self.max_points.update(max_points_per_resolution)

        # In-memory cache of local metrics for this worker, updated in
        # real-time and synced to DB.
        self.local_metrics_cache: Dict[str, Metric] = {}

        # In-memory cache of aggregated metrics from all workers, updated on
        # pulls from DB.
        self.aggregated_metrics_cache: Dict[str, Metric] = {}

        # Track metrics that have been modified locally since last sync.
        self.modified_metrics: Set[str] = set()

        # Last sync timestamp.
        self.last_sync_time = datetime.datetime.now(datetime.timezone.utc)

        # Locks to prevent race conditions.
        self.metric_locks: Dict[str, asyncio.Lock] = {}

        # Maximum age of metrics to keep in the database
        self.maximum_metric_age = maximum_metric_age

        # The interval at which to collect table size metrics
        self.collection_interval = collection_interval

    async def run(self, worker: Worker, chancy: Chancy):
        """
        Run the metrics plugin.

        This continuously synchronizes metrics with the database and
        other workers.
        """
        # Store the worker_id for use in metrics storage/retrieval
        self.worker_id = worker.worker_id

        self.aggregated_metrics_cache.update(
            await self._get_raw_metrics(chancy)
        )

        # Start a task to collect table size metrics
        worker.manager.add(
            f"metrics_table_sizes_{self.worker_id}",
            self._collect_table_sizes(worker, chancy),
        )

        while await self.sleep(self.sync_interval):
            await self._sync_metrics(chancy)

    def migrate_package(self) -> str:
        """
        Get the package that contains the migrations for the metrics plugin.
        """
        return "chancy.plugins.metrics.migrations"

    def migrate_key(self) -> str:
        """
        Get the unique identifier for this plugin's migrations.
        """
        return "metrics"

    async def on_job_updated(self, *, worker: "Worker", job: QueuedJob):
        if job.started_at and job.completed_at:
            execution_time = (job.completed_at - job.started_at).total_seconds()
            await self.record_histogram_value(
                f"job:{job.func}:execution_time",
                execution_time,
            )
            await self.record_histogram_value(
                f"queue:{job.queue}:execution_time",
                execution_time,
            )

        state = job.state.value
        await self.increment_counter(f"job:{job.func}:{state}", 1)
        await self.increment_counter(f"global:status:{state}", 1)
        await self.increment_counter(f"queue:{job.queue}:throughput", 1)
        await self.increment_counter(f"queue:{job.queue}:{state}", 1)

    async def cleanup(self, chancy: Chancy) -> Optional[int]:
        """
        Clean up old metrics data.

        Called automatically by the Pruner plugin, or may be manually invoked.
        """

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                query = sql.SQL(
                    """
                    DELETE
                    FROM {metrics_table}
                    WHERE
                        updated_at < NOW() - interval '{max_age} seconds'
                    """
                ).format(
                    metrics_table=sql.Identifier(f"{chancy.prefix}metrics"),
                    max_age=sql.Literal(self.maximum_metric_age),
                )

                await cursor.execute(query)
                return cursor.rowcount if cursor.rowcount > 0 else None

    async def _get_metric_lock(self, metric_key: str) -> asyncio.Lock:
        """Get a lock for a specific metric to prevent race conditions."""
        if metric_key not in self.metric_locks:
            self.metric_locks[metric_key] = asyncio.Lock()
        return self.metric_locks[metric_key]

    async def increment_counter(
        self, metric_key: str, value: Union[int, float]
    ) -> None:
        """
        Increment a counter metric.

        Counter metrics accumulate values over time periods.

        :param metric_key: The unique key for the metric
        :param value: The value to increment the counter by
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        async with await self._get_metric_lock(metric_key):
            if metric_key not in self.local_metrics_cache:
                self.local_metrics_cache[metric_key] = Metric(
                    metric_type="counter"
                )

            metric = self.local_metrics_cache[metric_key]

            for resolution in self.max_points.keys():
                match resolution:
                    case "1min":
                        points = metric.values_1min
                    case "5min":
                        points = metric.values_5min
                    case "1hour":
                        points = metric.values_1hour
                    case "1day":
                        points = metric.values_1day

                bucket_time = self._get_bucket_time(now, resolution)

                if points and self._same_bucket(
                    points[0][0], bucket_time, resolution
                ):
                    current_value = cast(Union[int, float], points[0][1])
                    points[0] = (points[0][0], current_value + value)
                else:
                    points.insert(0, (bucket_time, value))

                if len(points) > self.max_points[resolution]:
                    points.pop()

                self.modified_metrics.add(metric_key)

    async def record_gauge(
        self, metric_key: str, value: Union[int, float]
    ) -> None:
        """
        Record a gauge metric which represents a point-in-time value.

        Gauge metrics record the most recent value in each time bucket.

        :param metric_key: The unique key for the metric
        :param value: The value to record
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        async with await self._get_metric_lock(metric_key):
            if metric_key not in self.local_metrics_cache:
                self.local_metrics_cache[metric_key] = Metric(
                    metric_type="gauge"
                )

            metric = self.local_metrics_cache[metric_key]

            for resolution in self.max_points.keys():
                if resolution == "1min":
                    points = metric.values_1min
                elif resolution == "5min":
                    points = metric.values_5min
                elif resolution == "1hour":
                    points = metric.values_1hour
                elif resolution == "1day":
                    points = metric.values_1day

                bucket_time = self._get_bucket_time(now, resolution)

                if points and self._same_bucket(
                    points[0][0], bucket_time, resolution
                ):
                    points[0] = (points[0][0], value)
                else:
                    points.insert(0, (bucket_time, value))

                if len(points) > self.max_points[resolution]:
                    points.pop()

                self.modified_metrics.add(metric_key)

    async def record_histogram_value(
        self, metric_key: str, value: Union[int, float]
    ) -> None:
        """
        Record a value to a histogram metric.

        Histogram metrics track statistics (min, max, avg, count) for values
        over time periods.

        :param metric_key: The unique key for the metric
        :param value: The value to record
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        async with await self._get_metric_lock(metric_key):
            if metric_key not in self.local_metrics_cache:
                self.local_metrics_cache[metric_key] = Metric(
                    metric_type="histogram"
                )

            metric = self.local_metrics_cache[metric_key]

            for resolution in self.max_points.keys():
                if resolution == "1min":
                    points = metric.values_1min
                elif resolution == "5min":
                    points = metric.values_5min
                elif resolution == "1hour":
                    points = metric.values_1hour
                elif resolution == "1day":
                    points = metric.values_1day

                bucket_time = self._get_bucket_time(now, resolution)

                if points and self._same_bucket(
                    points[0][0], bucket_time, resolution
                ):
                    current_stats = cast(
                        Dict[str, Union[int, float]], points[0][1]
                    )

                    count = cast(int, current_stats.get("count", 0)) + 1
                    current_sum = (
                        cast(float, current_stats.get("sum", 0)) + value
                    )
                    current_min = min(
                        cast(float, current_stats.get("min", value)), value
                    )
                    current_max = max(
                        cast(float, current_stats.get("max", value)), value
                    )

                    new_stats = {
                        "count": count,
                        "sum": current_sum,
                        "avg": current_sum / count,
                        "min": current_min,
                        "max": current_max,
                    }
                    points[0] = (points[0][0], new_stats)
                else:
                    initial_stats = {
                        "count": 1,
                        "sum": value,
                        "avg": value,
                        "min": value,
                        "max": value,
                    }
                    points.insert(0, (bucket_time, initial_stats))

                if len(points) > self.max_points[resolution]:
                    points.pop()

                self.modified_metrics.add(metric_key)

    async def _sync_metrics(self, chancy: Chancy) -> None:
        """
        Synchronize metrics with the database and other workers.
        """
        if self.modified_metrics:
            await self._push_metrics_to_db(chancy)

        self.aggregated_metrics_cache.update(
            await self._get_raw_metrics(chancy)
        )
        self.last_sync_time = datetime.datetime.now(datetime.timezone.utc)
        self.modified_metrics.clear()

    async def _push_metrics_to_db(self, chancy: Chancy) -> None:
        """
        Push modified metrics to the database.

        Only pushes metrics from the local_metrics_cache, not the aggregated
        cache.
        """
        if not self.modified_metrics:
            return

        metrics_to_insert = []

        for metric_key in self.modified_metrics:
            if metric_key not in self.local_metrics_cache:
                continue

            metric = self.local_metrics_cache[metric_key]

            for resolution in self.max_points.keys():
                if resolution == "1min":
                    points = metric.values_1min
                elif resolution == "5min":
                    points = metric.values_5min
                elif resolution == "1hour":
                    points = metric.values_1hour
                elif resolution == "1day":
                    points = metric.values_1day
                else:
                    continue

                if not points:
                    continue

                timestamps = [point[0] for point in points]
                values = [json.dumps(point[1]) for point in points]

                metrics_to_insert.append(
                    {
                        "metric_key": metric_key,
                        "resolution": resolution,
                        "worker_id": self.worker_id,
                        "timestamps": timestamps,
                        "values": values,
                        "metric_type": metric.metric_type,
                    }
                )

        if not metrics_to_insert:
            return

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.executemany(
                    sql.SQL(
                        """
                        INSERT INTO {metrics_table} (
                            metric_key,
                            resolution,
                            worker_id,
                            timestamps,
                            values,
                            metric_type,
                            updated_at
                        ) VALUES (
                            %(metric_key)s,
                            %(resolution)s,
                            %(worker_id)s, 
                            %(timestamps)s,
                            %(values)s,
                            %(metric_type)s,
                            NOW()
                        )
                        ON CONFLICT (
                            metric_key,
                            resolution,
                            worker_id
                        ) DO UPDATE
                        SET
                            timestamps = %(timestamps)s,
                            values = %(values)s,
                            metric_type = %(metric_type)s,
                            updated_at = NOW()
                    """
                    ).format(
                        metrics_table=sql.Identifier(f"{chancy.prefix}metrics")
                    ),
                    metrics_to_insert,
                )

    @staticmethod
    def _get_bucket_time(
        timestamp: datetime.datetime, resolution: Resolution
    ) -> datetime.datetime:
        """
        Get the appropriate time bucket for a given timestamp and resolution.
        """
        if resolution == "1min":
            return timestamp.replace(second=0, microsecond=0)
        elif resolution == "5min":
            minute = (timestamp.minute // 5) * 5
            return timestamp.replace(minute=minute, second=0, microsecond=0)
        elif resolution == "1hour":
            return timestamp.replace(minute=0, second=0, microsecond=0)
        elif resolution == "1day":
            return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            raise ValueError(f"Unknown resolution: {resolution}")

    @staticmethod
    def _same_bucket(
        time1: datetime.datetime,
        time2: datetime.datetime,
        resolution: Resolution,
    ) -> bool:
        """
        Check if two timestamps belong to the same bucket for a given
        resolution.
        """
        if resolution == "1min":
            return (
                time1.year == time2.year
                and time1.month == time2.month
                and time1.day == time2.day
                and time1.hour == time2.hour
                and time1.minute == time2.minute
            )
        elif resolution == "5min":
            return (
                time1.year == time2.year
                and time1.month == time2.month
                and time1.day == time2.day
                and time1.hour == time2.hour
                and time1.minute // 5 == time2.minute // 5
            )
        elif resolution == "1hour":
            return (
                time1.year == time2.year
                and time1.month == time2.month
                and time1.day == time2.day
                and time1.hour == time2.hour
            )
        elif resolution == "1day":
            return (
                time1.year == time2.year
                and time1.month == time2.month
                and time1.day == time2.day
            )
        else:
            raise ValueError(f"Unknown resolution: {resolution}")

    def api_plugin(self) -> str | None:
        return "chancy.plugins.metrics.api.MetricsApiPlugin"

    def get_tables(self) -> list[str]:
        """Get the names of all tables this plugin is responsible for."""
        return ["metrics"]

    @staticmethod
    def get_identifier() -> str:
        return "chancy.metrics"

    async def _collect_table_sizes(self, worker: Worker, chancy: Chancy):
        """
        Collect size metrics for database tables.

        This runs as a separate task and collects table sizes every 30 seconds,
        but only when this worker is the leader to avoid duplicate metrics.
        """
        core_tables = [
            "jobs",
            "queues",
            "workers",
            "leader",
            "queue_rate_limits",
        ]

        while await self.sleep(self.collection_interval):
            await self.wait_for_leader(worker)

            plugin_tables = []
            for plugin in chancy.plugins.values():
                plugin_tables.extend(plugin.get_tables())

            all_tables = list(set(plugin_tables + core_tables))
            prefixed_tables = [
                f"{chancy.prefix}{table}" for table in all_tables
            ]

            async with chancy.pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cursor:
                    query = sql.SQL(
                        """
                        SELECT
                            table_name,
                            pg_total_relation_size(table_name) as total_size_bytes,
                            pg_relation_size(table_name) as table_size_bytes,
                            pg_indexes_size(table_name) as index_size_bytes
                        FROM unnest({tables}::text[]) AS table_name
                    """
                    ).format(tables=sql.Literal(prefixed_tables))

                    await cursor.execute(query)

                    async for result in cursor:
                        table_name = result["table_name"].removeprefix(
                            chancy.prefix
                        )

                        await self.record_histogram_value(
                            f"table:{table_name}:total_size_bytes",
                            result["total_size_bytes"],
                        )
                        await self.record_histogram_value(
                            f"table:{table_name}:table_size_bytes",
                            result["table_size_bytes"],
                        )
                        await self.record_histogram_value(
                            f"table:{table_name}:index_size_bytes",
                            result["index_size_bytes"],
                        )

    async def _get_raw_metrics(
        self,
        chancy: Chancy,
        *,
        metric_prefix: str | None = None,
        worker_id: str | None = None,
    ) -> Dict[str, Metric]:
        results = {}

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT 
                            metric_key,
                            resolution,
                            metric_type,
                            json_agg(timestamps ORDER BY worker_id, updated_at) as all_timestamps,
                            json_agg(values ORDER BY worker_id, updated_at) as all_values
                        FROM 
                            {metrics_table}
                        WHERE
                            (
                                %(worker_id)s::text IS NULL OR
                                    worker_id = %(worker_id)s
                            )
                        AND
                            (
                                %(metric_prefix)s::text IS NULL OR
                                    metric_key LIKE %(metric_prefix)s
                            )
                        GROUP BY 
                            metric_key, resolution, metric_type
                        ORDER BY 
                            metric_key, resolution
                        """
                    ).format(
                        metrics_table=sql.Identifier(f"{chancy.prefix}metrics"),
                    ),
                    {
                        "worker_id": worker_id,
                        "metric_prefix": (
                            f"{metric_prefix}%" if metric_prefix else None
                        ),
                    },
                )

                for metric_key, group in groupby(
                    [r async for r in cursor],
                    key=lambda r: r["metric_key"],
                ):
                    if metric_prefix and not self.matches_prefix(
                        metric_prefix, metric_key
                    ):
                        continue

                    group_list = list(group)

                    metric = results.setdefault(
                        metric_key,
                        Metric(metric_type=group_list[0]["metric_type"]),
                    )

                    for row in group_list:
                        resolution = cast(Resolution, row["resolution"])
                        merged_points = defaultdict(list)

                        for worker_idx, timestamps_array in enumerate(
                            row["all_timestamps"]
                        ):
                            values_array = row["all_values"][worker_idx]

                            for i in range(len(timestamps_array)):
                                timestamp = timestamps_array[i]
                                merged_points[timestamp].append(values_array[i])

                        result_points = []
                        for timestamp, values_list in merged_points.items():
                            if metric.metric_type == "counter":
                                merged_value = sum(values_list)
                            elif metric.metric_type == "gauge":
                                merged_value = values_list[0]
                            elif metric.metric_type == "histogram":
                                total_count = sum(
                                    v.get("count", 0) for v in values_list
                                )
                                total_sum = sum(
                                    v.get("sum", 0) for v in values_list
                                )

                                all_mins = [
                                    v.get("min")
                                    for v in values_list
                                    if "min" in v
                                ]
                                all_maxs = [
                                    v.get("max")
                                    for v in values_list
                                    if "max" in v
                                ]

                                merged_value = {
                                    "count": total_count,
                                    "sum": total_sum,
                                    "avg": (
                                        total_sum / total_count
                                        if total_count > 0
                                        else 0
                                    ),
                                    "min": min(all_mins) if all_mins else 0,
                                    "max": max(all_maxs) if all_maxs else 0,
                                }

                            result_points.append((timestamp, merged_value))

                        result_points.sort(key=lambda p: p[0], reverse=True)

                        max_points = self.max_points[resolution]
                        match resolution:
                            case "1min":
                                metric.values_1min = result_points[:max_points]
                            case "5min":
                                metric.values_5min = result_points[:max_points]
                            case "1hour":
                                metric.values_1hour = result_points[:max_points]
                            case "1day":
                                metric.values_1day = result_points[:max_points]

        return results

    async def get_metrics(
        self,
        chancy: Chancy,
        metric_prefix: str | None = None,
        worker_id: str | None = None,
    ) -> Dict[str, Metric]:
        """
        Get metrics matching the given prefix.

        If a worker_id is provided, only metrics for that worker are returned
        and are queried directly from the database. Otherwise, aggregated
        metrics are returned from the metrics cache.

        :param chancy: The Chancy application instance
        :param metric_prefix: Optional prefix to filter metrics by
        :param worker_id: Optional worker_id to filter metrics by
        """
        if worker_id and worker_id == self.worker_id:
            return {
                key: metric
                for key, metric in self.local_metrics_cache.items()
                if not metric_prefix or self.matches_prefix(metric_prefix, key)
            }
        elif worker_id:
            return await self._get_raw_metrics(
                chancy,
                metric_prefix=metric_prefix,
                worker_id=worker_id,
            )
        else:
            return {
                key: metric
                for key, metric in self.aggregated_metrics_cache.items()
                if not metric_prefix or self.matches_prefix(metric_prefix, key)
            }

    @staticmethod
    def matches_prefix(prefix: str, key: str) -> bool:
        """
        Check if the given key matches the given complete prefix.

        :param prefix: The prefix to match
        :param key: The key to check
        """
        key = key.split(":")
        prefix = prefix.split(":")
        return key[: len(prefix)] == prefix
