from collections import defaultdict

from starlette.requests import Request
from starlette.responses import Response

from chancy.plugins.api.plugin import ApiPlugin
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
                "path": "/api/v1/metrics/{prefix}",
                "endpoint": self.get_metric_detail,
                "methods": ["GET"],
                "name": "get_metric_detail",
            },
        ]

    @staticmethod
    async def get_metrics(request: Request, *, chancy, worker):
        """
        Get a list of all available metrics.
        """
        metrics_plugin = chancy.plugins["chancy.metrics"]

        metric_categories = defaultdict(list)

        metrics = await metrics_plugin.get_metrics(chancy)
        for key in metrics.keys():
            parts = key.split(":")
            category, metric_name = parts[0], parts[1]
            if metric_name not in metric_categories[category]:
                metric_categories[category].append(metric_name)

        return Response(
            json_dumps(
                {
                    "categories": metric_categories,
                    "count": len(metrics),
                }
            ),
            media_type="application/json",
        )

    @staticmethod
    async def get_metric_detail(request: Request, *, chancy, worker):
        """
        Get detailed data for a specific metric.

        Can be filtered by worker_id with the worker_id query parameter.
        """
        metrics_plugin = chancy.plugins["chancy.metrics"]

        metric_prefix = request.path_params.get("prefix", "")
        resolution = request.query_params.get("resolution", "5min")
        worker_id = request.query_params.get("worker_id")

        metrics = await metrics_plugin.get_metrics(
            chancy,
            metric_prefix=metric_prefix,
            worker_id=worker_id,
        )

        return Response(
            json_dumps(
                {
                    metric_key: {
                        "data": [
                            {
                                "timestamp": timestamp,
                                "value": value,
                            }
                            for timestamp, value in getattr(
                                metric, f"values_{resolution}"
                            )
                        ],
                        "type": metric.metric_type,
                    }
                    for metric_key, metric in metrics.items()
                }
            ),
            media_type="application/json",
        )
