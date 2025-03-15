"""
Metrics plugin for collecting and sharing metrics across workers.
"""

import asyncio
import datetime
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, Union, cast

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
    A plugin that collects and aggregates various metrics from jobs and queues.

    The plugin maintains time-series data for various metrics, with automatic
    aggregation and pruning to keep storage requirements low while providing
    useful historical data.

    Metrics are synchronized across workers, so each worker has access to the
    full set of metrics.

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
        monitoring tool like Prometheus or Grafana.
    """

    def __init__(
        self,
        *,
        sync_interval: int = 60,
        max_points_per_resolution: Dict[Resolution, int] = None,
    ):
        """
        Initialize the metrics plugin.

        :param sync_interval: How often to synchronize metrics with the
                              database and other workers.
        :param max_points_per_resolution: How many points to keep for each
                                          resolution.
        """
        super().__init__()
        self.sync_interval = sync_interval

        # Worker ID will be set in run()
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

    async def run(self, worker: Worker, chancy: Chancy):
        """
        Run the metrics plugin.

        This continuously synchronizes metrics with the database and
        other workers.
        """
        # Store the worker_id for use in metrics storage/retrieval
        self.worker_id = worker.worker_id

        # Initial load of metrics from the database
        await self._load_metrics_from_db(chancy)

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

    async def on_job_completed(
        self,
        *,
        worker: Worker,
        job: QueuedJob,
        exc: Exception = None,
        result: Any = None,
    ) -> QueuedJob:
        """
        Track job completion metrics.

        Updates several metrics:
        1. Job success/failure count by function
        2. Queue throughput
        3. Job execution time
        4. Global job status counts
        """
        execution_time = None
        if job.started_at and job.completed_at:
            execution_time = (job.completed_at - job.started_at).total_seconds()

        await self.increment_counter(
            f"job:{job.func}:{'success' if exc is None else 'failure'}",
            1,
        )

        # Increment global job status counter
        await self.increment_counter(f"global:status:{job.state.value}", 1)

        await self.increment_counter(f"queue:{job.queue}:throughput", 1)

        if execution_time is not None:
            await self.record_histogram_value(
                f"job:{job.func}:execution_time", execution_time
            )
            await self.record_histogram_value(
                f"queue:{job.queue}:execution_time",
                execution_time,
            )

        return job

    async def cleanup(self, chancy: Chancy) -> Optional[int]:
        """
        Clean up old metrics data.

        Called by the Pruner plugin.
        """
        pruned_count = 0

        async with chancy.pool.connection() as conn:
            async with conn.cursor() as cursor:
                for resolution, max_points in self.max_points.items():
                    # Query to prune the arrays, keeping only the most recent
                    # max_points.
                    query = sql.SQL(
                        """
                        UPDATE {metrics_table}
                        SET
                            timestamps = (
                                SELECT ARRAY(
                                    SELECT unnest(timestamps) 
                                    ORDER BY unnest DESC 
                                    LIMIT {max_points}
                                )
                            ),
                            values = (
                                SELECT ARRAY(
                                    SELECT unnest(values)
                                    FROM (
                                        SELECT ROW_NUMBER() OVER (ORDER BY unnest(timestamps) DESC) as rn,
                                               unnest(values) as unnest
                                        FROM {metrics_table} m2
                                        WHERE m2.metric_key = {metrics_table}.metric_key
                                        AND m2.resolution = {metrics_table}.resolution
                                    ) ranked
                                    WHERE rn <= {max_points}
                                )
                            )
                        WHERE
                            resolution = {resolution}
                            AND array_length(timestamps, 1) > {max_points}
                    """
                    ).format(
                        metrics_table=sql.Identifier(f"{chancy.prefix}metrics"),
                        max_points=sql.Literal(max_points),
                        resolution=sql.Literal(resolution),
                    )

                    await cursor.execute(query)
                    pruned_count += cursor.rowcount

        return pruned_count if pruned_count > 0 else None

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

                    # Update the stats
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
                    # Create a new histogram bucket
                    initial_stats = {
                        "count": 1,
                        "sum": value,
                        "avg": value,
                        "min": value,
                        "max": value,
                    }
                    points.insert(0, (bucket_time, initial_stats))

                # Prune if we have too many points
                if len(points) > self.max_points[resolution]:
                    points.pop()

                # Mark this metric as modified
                self.modified_metrics.add(metric_key)

    async def _sync_metrics(self, chancy: Chancy) -> None:
        """
        Synchronize metrics with the database and other workers.
        """
        if self.modified_metrics:
            await self._push_metrics_to_db(chancy)

        await self._load_metrics_from_db(chancy, load_only_changes=True)
        self.last_sync_time = datetime.datetime.now(datetime.timezone.utc)
        self.modified_metrics.clear()

    async def _push_metrics_to_db(self, chancy: Chancy) -> None:
        """
        Push modified metrics to the database.

        Only pushes metrics from the local_metrics_cache, not the aggregated
        cache. Uses a bulk insert approach for better performance.
        """
        if not self.modified_metrics:
            return

        metrics_to_insert = []

        # Process each metric key that was modified
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
            async with conn.cursor() as cursor:
                # Execute a single query with all metrics using execute_batch
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
                            %(metric_key)s, %(resolution)s, %(worker_id)s, 
                            %(timestamps)s, %(values)s, %(metric_type)s, NOW()
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

    async def _load_metrics_from_db(
        self, chancy: Chancy, load_only_changes: bool = False
    ) -> None:
        """
        Load metrics from the database.

        This method retrieves metrics from all workers and merges them together
        into the aggregated_metrics_cache.

        :param chancy: The Chancy application instance
        :param load_only_changes: If True, only load metrics updated since the
                                  last sync time
        """
        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                if load_only_changes:
                    query = sql.SQL(
                        """
                        SELECT 
                            metric_key,
                            resolution,
                            worker_id,
                            timestamps,
                            values,
                            metric_type
                        FROM 
                            {metrics_table}
                        WHERE 
                            updated_at > %s
                        ORDER BY 
                            metric_key, resolution
                        """
                    ).format(
                        metrics_table=sql.Identifier(f"{chancy.prefix}metrics")
                    )
                    await cursor.execute(query, (self.last_sync_time,))
                else:
                    query = sql.SQL(
                        """
                        SELECT 
                            metric_key,
                            resolution,
                            worker_id,
                            timestamps,
                            values,
                            metric_type
                        FROM 
                            {metrics_table}
                        ORDER BY 
                            metric_key, resolution
                        """
                    ).format(
                        metrics_table=sql.Identifier(f"{chancy.prefix}metrics")
                    )
                    await cursor.execute(query)

                # Group the results by metric_key and resolution
                metrics_data = {}
                metric_types = {}
                async for row in cursor:
                    metric_key = row["metric_key"]
                    resolution = cast(Resolution, row["resolution"])
                    # Store the metric type - it should always be present
                    metric_type = row["metric_type"]
                    if not metric_type:
                        raise ValueError(
                            f"Metric {metric_key} is missing required metric_type"
                        )
                    metric_types[metric_key] = cast(MetricType, metric_type)

                    if metric_key not in metrics_data:
                        metrics_data[metric_key] = {}

                    if resolution not in metrics_data[metric_key]:
                        metrics_data[metric_key][resolution] = []

                    metrics_data[metric_key][resolution].append(row)

                # Process each metric_key and resolutions
                for metric_key, resolutions in metrics_data.items():
                    # Get the metric type, default to counter if not found
                    metric_type = metric_types.get(metric_key, "counter")

                    # Initialize result dicts for each resolution
                    values_1min = []
                    values_5min = []
                    values_1hour = []
                    values_1day = []

                    # Process each resolution
                    for resolution, worker_data in resolutions.items():
                        # Collect all points from all workers
                        all_points = []
                        for row in worker_data:
                            timestamps = row["timestamps"]
                            values = row["values"]

                            for i in range(len(timestamps)):
                                value = (
                                    json.loads(values[i])
                                    if isinstance(values[i], str)
                                    else values[i]
                                )
                                all_points.append((timestamps[i], value))

                        # Group points by timestamp for merging
                        merged_points: Dict[
                            datetime.datetime, List[MetricValue]
                        ] = {}
                        for timestamp, value in all_points:
                            if timestamp not in merged_points:
                                merged_points[timestamp] = []
                            merged_points[timestamp].append(value)

                        result_points = []
                        for timestamp, values_list in merged_points.items():
                            # If all values are numbers, sum them
                            if all(
                                isinstance(v, (int, float)) for v in values_list
                            ):
                                merged_value = sum(values_list)
                            # If all values are dictionaries with the same keys, merge them
                            elif all(isinstance(v, dict) for v in values_list):
                                # Handle histogram values (with stats)
                                if all(
                                    "count" in v for v in values_list
                                ) and all("sum" in v for v in values_list):
                                    total_count = sum(
                                        v.get("count", 0) for v in values_list
                                    )
                                    total_sum = sum(
                                        v.get("sum", 0) for v in values_list
                                    )

                                    # Find min/max across all workers
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
                                else:
                                    merged_value = {}
                                    for v in values_list:
                                        for k, val in v.items():
                                            if k in merged_value:
                                                merged_value[k] += val
                                            else:
                                                merged_value[k] = val
                            else:
                                merged_value = values_list[0]

                            result_points.append((timestamp, merged_value))

                        # Sort points by timestamp (newest first)
                        result_points.sort(key=lambda p: p[0], reverse=True)

                        # Store points in the appropriate resolution list
                        if resolution == "1min":
                            values_1min = result_points[
                                : self.max_points[resolution]
                            ]
                        elif resolution == "5min":
                            values_5min = result_points[
                                : self.max_points[resolution]
                            ]
                        elif resolution == "1hour":
                            values_1hour = result_points[
                                : self.max_points[resolution]
                            ]
                        elif resolution == "1day":
                            values_1day = result_points[
                                : self.max_points[resolution]
                            ]

                    # Create and store the Metric object in the aggregated cache
                    self.aggregated_metrics_cache[metric_key] = Metric(
                        metric_type=cast(MetricType, metric_type),
                        values_1min=values_1min,
                        values_5min=values_5min,
                        values_1hour=values_1hour,
                        values_1day=values_1day,
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

    def get_metrics(
        self,
        metric_prefix: Optional[str] = None,
        resolution: Optional[Resolution] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Dict[Resolution, List[MetricPoint]]]:
        """
        Get metrics matching the given prefix.

        :param metric_prefix: Optional prefix to filter metrics by
        :param resolution: Optional resolution to filter by
        :param limit: Optional limit on the number of points to return per
                      metric
        """
        result = {}

        # Use the aggregated metrics cache for queries
        for key, metric in self.aggregated_metrics_cache.items():
            if metric_prefix and not key.startswith(metric_prefix):
                continue

            result[key] = {}

            resolutions = metric.values
            for res, points in resolutions.items():
                if resolution and res != resolution:
                    continue

                if limit:
                    result[key][res] = points[:limit]
                else:
                    result[key][res] = points

        return result

    def get_metric_types(
        self, metric_prefix: Optional[str] = None
    ) -> Dict[str, MetricType]:
        """
        Get the type of each metric.

        :param metric_prefix: Optional prefix to filter metrics by
        :return: Dictionary mapping metric keys to their types
        """
        result = {}

        for key, metric in self.aggregated_metrics_cache.items():
            if metric_prefix and not key.startswith(metric_prefix):
                continue

            result[key] = metric.metric_type

        return result
