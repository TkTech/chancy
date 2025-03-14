"""
Metrics plugin for collecting and sharing metrics across workers.
"""

import asyncio
import datetime
import json
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


class Metrics(Plugin):
    """
    A plugin that collects and aggregates metrics from jobs and queues.

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

    By default, this plugin will:

    1. Track job success/failure counts by function name
    2. Track queue throughput (jobs processed per time period)
    3. Track job execution time statistics
    4. Synchronize metrics between workers every 60 seconds

    The metrics are stored in a compact time-series format, with data points
    aggregated at different resolutions (1 minute, 5 minutes, 1 hour, 1 day).
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

        # In-memory cache of local metrics for this worker, updated in real-time and synced to DB
        self.local_metrics_cache: Dict[
            str, Dict[Resolution, List[MetricPoint]]
        ] = {}

        # In-memory cache of aggregated metrics from all workers, updated on pulls from DB
        self.aggregated_metrics_cache: Dict[
            str, Dict[Resolution, List[MetricPoint]]
        ] = {}

        # Track metrics that have been modified locally since last sync
        self.modified_metrics: Set[Tuple[str, Resolution]] = set()

        # Last sync timestamp
        self.last_sync_time = datetime.datetime.now(datetime.timezone.utc)

        # Locks to prevent race conditions
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
        """
        execution_time = None
        if job.started_at and job.completed_at:
            execution_time = (job.completed_at - job.started_at).total_seconds()

        await self.increment_counter(
            f"job:{job.func}:{'success' if exc is None else 'failure'}",
            1,
            worker.chancy,
        )

        await self.increment_counter(
            f"queue:{job.queue}:throughput", 1, worker.chancy
        )

        if execution_time is not None:
            await self.record_histogram_value(
                f"job:{job.func}:execution_time", execution_time, worker.chancy
            )
            await self.record_histogram_value(
                f"queue:{job.queue}:execution_time",
                execution_time,
                worker.chancy,
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
        self, metric_key: str, value: Union[int, float], chancy: Chancy
    ) -> None:
        """
        Increment a counter metric.

        Counter metrics accumulate values over time periods.
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        async with await self._get_metric_lock(metric_key):
            if metric_key not in self.local_metrics_cache:
                self.local_metrics_cache[metric_key] = {
                    resolution: [] for resolution in self.max_points.keys()
                }

            for resolution in self.local_metrics_cache[metric_key]:
                points = self.local_metrics_cache[metric_key][resolution]

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

                self.modified_metrics.add((metric_key, resolution))

    async def record_gauge(
        self, metric_key: str, value: Union[int, float], chancy: Chancy
    ) -> None:
        """
        Record a gauge metric which represents a point-in-time value.

        Gauge metrics record the most recent value in each time bucket.
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        async with await self._get_metric_lock(metric_key):
            if metric_key not in self.local_metrics_cache:
                self.local_metrics_cache[metric_key] = {
                    resolution: [] for resolution in self.max_points.keys()
                }

            for resolution in self.local_metrics_cache[metric_key]:
                points = self.local_metrics_cache[metric_key][resolution]

                bucket_time = self._get_bucket_time(now, resolution)

                if points and self._same_bucket(
                    points[0][0], bucket_time, resolution
                ):
                    points[0] = (points[0][0], value)
                else:
                    points.insert(0, (bucket_time, value))

                if len(points) > self.max_points[resolution]:
                    points.pop()

                self.modified_metrics.add((metric_key, resolution))

    async def record_histogram_value(
        self, metric_key: str, value: Union[int, float], chancy: Chancy
    ) -> None:
        """
        Record a value to a histogram metric.

        Histogram metrics track statistics (min, max, avg, count) for values
        over time periods.
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        async with await self._get_metric_lock(metric_key):
            if metric_key not in self.local_metrics_cache:
                self.local_metrics_cache[metric_key] = {
                    resolution: [] for resolution in self.max_points.keys()
                }

            for resolution in self.local_metrics_cache[metric_key]:
                points = self.local_metrics_cache[metric_key][resolution]

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
                self.modified_metrics.add((metric_key, resolution))

    async def _sync_metrics(self, chancy: Chancy) -> None:
        """
        Synchronize metrics with the database and other workers.
        """
        if not self.modified_metrics:
            return

        await self._push_metrics_to_db(chancy)
        await self._load_metrics_from_db(chancy, load_only_changes=True)
        self.last_sync_time = datetime.datetime.now(datetime.timezone.utc)
        self.modified_metrics.clear()

    async def _push_metrics_to_db(self, chancy: Chancy) -> None:
        """
        Push modified metrics to the database.

        Only pushes metrics from the local_metrics_cache, not the aggregated cache.
        """
        if not self.modified_metrics:
            return

        chancy.log.info("Pushing metrics to the database...")

        async with chancy.pool.connection() as conn:
            for metric_key, resolution in self.modified_metrics:
                if (
                    metric_key not in self.local_metrics_cache
                    or not self.local_metrics_cache[metric_key].get(resolution)
                ):
                    continue

                points = self.local_metrics_cache[metric_key][resolution]
                if not points:
                    continue

                timestamps = [point[0] for point in points]
                values = [json.dumps(point[1]) for point in points]

                async with conn.cursor() as cursor:
                    await cursor.execute(
                        sql.SQL(
                            """
                            INSERT INTO {metrics_table} (
                                metric_key, resolution, worker_id, timestamps, values, updated_at
                            ) VALUES (
                                %s, %s, %s, %s, %s, NOW()
                            )
                            ON CONFLICT (metric_key, resolution, worker_id) DO UPDATE
                            SET
                                timestamps = %s,
                                values = %s,
                                updated_at = NOW()
                        """
                        ).format(
                            metrics_table=sql.Identifier(
                                f"{chancy.prefix}metrics"
                            )
                        ),
                        (
                            metric_key,
                            resolution,
                            self.worker_id,
                            timestamps,
                            values,
                            timestamps,
                            values,
                        ),
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
                # Get unique metric_key/resolution combinations
                if load_only_changes:
                    # Only get metrics updated since last sync
                    await cursor.execute(
                        sql.SQL(
                            """
                            SELECT DISTINCT
                                metric_key, resolution
                            FROM
                                {metrics_table}
                            WHERE
                                updated_at > %s
                            ORDER BY
                                metric_key, resolution
                            """
                        ).format(
                            metrics_table=sql.Identifier(
                                f"{chancy.prefix}metrics"
                            )
                        ),
                        (self.last_sync_time,),
                    )
                else:
                    # Get all metrics
                    await cursor.execute(
                        sql.SQL(
                            """
                            SELECT DISTINCT
                                metric_key, resolution
                            FROM
                                {metrics_table}
                            ORDER BY
                                metric_key, resolution
                            """
                        ).format(
                            metrics_table=sql.Identifier(
                                f"{chancy.prefix}metrics"
                            )
                        )
                    )

                metric_combinations = await cursor.fetchall()

                # For each combination, get and merge worker-specific data
                for combo in metric_combinations:
                    metric_key = combo["metric_key"]
                    resolution = cast(Resolution, combo["resolution"])

                    # Query all worker data for this metric
                    await cursor.execute(
                        sql.SQL(
                            """
                            SELECT
                                worker_id, timestamps, values
                            FROM
                                {metrics_table}
                            WHERE
                                metric_key = %s AND
                                resolution = %s
                            """
                        ).format(
                            metrics_table=sql.Identifier(
                                f"{chancy.prefix}metrics"
                            )
                        ),
                        (metric_key, resolution),
                    )

                    worker_data = await cursor.fetchall()

                    # Initialize the metric in aggregated cache if needed
                    if metric_key not in self.aggregated_metrics_cache:
                        self.aggregated_metrics_cache[metric_key] = {
                            cast(Resolution, res): []
                            for res in self.max_points.keys()
                        }

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
                            if all("count" in v for v in values_list) and all(
                                "sum" in v for v in values_list
                            ):
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
                                # For other dict types, merge by summing values with same keys
                                merged_value = {}
                                for v in values_list:
                                    for k, val in v.items():
                                        if k in merged_value:
                                            merged_value[k] += val
                                        else:
                                            merged_value[k] = val
                        # Otherwise just use the first value (should not typically happen)
                        else:
                            merged_value = values_list[0]

                        result_points.append((timestamp, merged_value))

                    # Sort and update cache
                    result_points.sort(key=lambda p: p[0], reverse=True)
                    self.aggregated_metrics_cache[metric_key][resolution] = (
                        result_points[: self.max_points[resolution]]
                    )

    def _get_bucket_time(
        self, timestamp: datetime.datetime, resolution: Resolution
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

    def _same_bucket(
        self,
        time1: datetime.datetime,
        time2: datetime.datetime,
        resolution: Resolution,
    ) -> bool:
        """
        Check if two timestamps belong to the same bucket for a given resolution.
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
        for key, resolutions in self.aggregated_metrics_cache.items():
            if metric_prefix and not key.startswith(metric_prefix):
                continue

            result[key] = {}

            for res, points in resolutions.items():
                if resolution and res != resolution:
                    continue

                if limit:
                    result[key][res] = points[:limit]
                else:
                    result[key][res] = points

        return result
