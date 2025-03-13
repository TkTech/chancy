"""
API endpoints for the metrics plugin.
"""

import json
from typing import Optional

from psycopg import sql
from psycopg.rows import dict_row
from starlette.requests import Request
from starlette.responses import Response

from chancy.plugins.api.plugin import ApiPlugin
from chancy.plugins.metrics.metrics import Metrics
from chancy.utils import json_dumps


class MetricsApiPlugin(ApiPlugin):
    """
    API plugin for exposing metrics data.
    """

    def name(self):
        return "metrics"

    def routes(self):
        return [
            {
                "path": "/api/v1/metrics",
                "endpoint": self.get_metrics,
                "methods": ["GET"],
                "name": "get_metrics",
            },
            {
                "path": "/api/v1/metrics/{type}",
                "endpoint": self.get_metrics_by_type,
                "methods": ["GET"],
                "name": "get_metrics_by_type",
            },
            {
                "path": "/api/v1/metrics/{type}/{name}",
                "endpoint": self.get_metric_detail,
                "methods": ["GET"],
                "name": "get_metric_detail",
            },
        ]

    def get_metrics_plugin(self, chancy) -> Optional[Metrics]:
        """
        Get the metrics plugin instance.
        """
        for plugin in chancy.plugins:
            if isinstance(plugin, Metrics):
                return plugin
        return None

    @staticmethod
    async def get_metrics(request: Request, *, chancy, worker):
        """
        Get a list of all available metrics.
        """
        metrics_plugin = MetricsApiPlugin().get_metrics_plugin(chancy)
        if not metrics_plugin:
            return Response(
                json_dumps({"error": "Metrics plugin not enabled"}),
                status_code=404,
                media_type="application/json",
            )

        # Get a list of all available metrics
        metrics = metrics_plugin.get_metrics()

        # Get the types as a organized structure
        metric_types = {}
        for key in metrics.keys():
            parts = key.split(":")
            if len(parts) >= 2:
                metric_type = parts[0]
                metric_name = parts[1]

                if metric_type not in metric_types:
                    metric_types[metric_type] = []

                if metric_name not in metric_types[metric_type]:
                    metric_types[metric_type].append(metric_name)

        return Response(
            json_dumps(
                {
                    "types": metric_types,
                    "count": len(metrics),
                }
            ),
            media_type="application/json",
        )

    @staticmethod
    async def get_metrics_by_type(request: Request, *, chancy, worker):
        """
        Get metrics of a specific type (e.g., 'job', 'queue').
        """
        metrics_plugin = MetricsApiPlugin().get_metrics_plugin(chancy)
        if not metrics_plugin:
            return Response(
                json_dumps({"error": "Metrics plugin not enabled"}),
                status_code=404,
                media_type="application/json",
            )

        metric_type = request.path_params.get("type", "")
        resolution = request.query_params.get("resolution", "5min")
        limit = int(request.query_params.get("limit", "60"))

        # Get metrics with the specified prefix
        metrics = metrics_plugin.get_metrics(
            metric_prefix=f"{metric_type}:", resolution=resolution, limit=limit
        )

        # Transform metrics data for API response
        result = {}
        for key, resolutions in metrics.items():
            name = key.split(":")[1] if len(key.split(":")) > 1 else key
            metric_data = []

            for res, points in resolutions.items():
                if res == resolution:
                    for timestamp, value in points:
                        metric_data.append(
                            {"timestamp": timestamp.isoformat(), "value": value}
                        )

            result[name] = metric_data

        return Response(
            json_dumps(result),
            media_type="application/json",
        )

    @staticmethod
    async def get_metric_detail(request: Request, *, chancy, worker):
        """
        Get detailed data for a specific metric.
        
        Can be filtered by worker_id with the worker_id query parameter.
        """
        metrics_plugin = MetricsApiPlugin().get_metrics_plugin(chancy)
        if not metrics_plugin:
            return Response(
                json_dumps({"error": "Metrics plugin not enabled"}),
                status_code=404,
                media_type="application/json",
            )

        metric_type = request.path_params.get("type", "")
        metric_name = request.path_params.get("name", "")
        resolution = request.query_params.get("resolution", "5min")
        limit = int(request.query_params.get("limit", "60"))
        worker_id = request.query_params.get("worker_id")

        metric_key = f"{metric_type}:{metric_name}"
        
        # If worker_id is provided, query metrics directly from the database for that worker
        if worker_id:
            result = {}
            async with chancy.pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute(
                        sql.SQL(
                            """
                            SELECT 
                                metric_key, timestamps, values
                            FROM 
                                {metrics_table}
                            WHERE 
                                worker_id = %s AND
                                resolution = %s AND
                                metric_key LIKE %s
                            LIMIT 100
                            """
                        ).format(
                            metrics_table=sql.Identifier(f"{chancy.prefix}metrics")
                        ),
                        (worker_id, resolution, f"{metric_key}%"),
                    )
                    
                    rows = await cursor.fetchall()
                    
                    # Process results
                    for row in rows:
                        row_metric_key = row["metric_key"]
                        timestamps = row["timestamps"]
                        values = row["values"]
                        
                        # Extract the sub key (part after the metric_key)
                        subkey = row_metric_key[len(metric_key):].lstrip(":")
                        if not subkey:
                            subkey = "default"
                            
                        # Process the points
                        metric_data = []
                        for i in range(min(limit, len(timestamps))):
                            value = (
                                json.loads(values[i])
                                if isinstance(values[i], str)
                                else values[i]
                            )
                            metric_data.append(
                                {"timestamp": timestamps[i].isoformat(), "value": value}
                            )
                        
                        result[subkey] = metric_data
        else:
            # Get all metrics matching this pattern (could include multiple submetrics)
            metrics = metrics_plugin.get_metrics(
                metric_prefix=metric_key, resolution=resolution, limit=limit
            )

            # Transform metrics data for API response
            result = {}
            for key, resolutions in metrics.items():
                # Extract the part after "type:name:"
                subkey = key[len(metric_key) :].lstrip(":")

                if not subkey:  # This is the exact match
                    subkey = "default"

                metric_data = []
                for res, points in resolutions.items():
                    if res == resolution:
                        for timestamp, value in points:
                            metric_data.append(
                                {"timestamp": timestamp.isoformat(), "value": value}
                            )

                result[subkey] = metric_data

        if not result:
            return Response(
                json_dumps({"error": f"Metric {metric_key} not found"}),
                status_code=404,
                media_type="application/json",
            )

        return Response(
            json_dumps(result),
            media_type="application/json",
        )

