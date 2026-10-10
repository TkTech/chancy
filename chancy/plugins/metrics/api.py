from collections import defaultdict
from dataclasses import asdict
from datetime import datetime

from starlette.authentication import requires
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
    @requires(["authenticated"])
    async def get_metrics(request, *, chancy, worker):
        """
        Get a list of all available metrics, grouped by category.
        """
        keys = await chancy.plugins["chancy.metrics"].list_metrics(chancy)
        categories = defaultdict(set)
        for key in keys:
            category, _, name = key.partition(":")
            categories[category].add(name.split(":", 1)[0])
        return Response(
            json_dumps(
                {
                    "categories": {k: sorted(v) for k, v in categories.items()},
                    "count": len(keys),
                }
            ),
            media_type="application/json",
        )

    @staticmethod
    @requires(["authenticated"])
    async def get_metric_detail(request, *, chancy, worker):
        """
        Get detailed data for a metric or group of metrics.

        Use the ``worker_id`` query parameter to show only one worker's data.
        ``resolution`` controls the size of each time bucket; ``range`` controls
        how much history to return, in seconds. For example,
        ``?resolution=5min&range=86400`` returns 24 hours of five-minute data.

        Alternatively, provide timezone-aware ISO ``start`` and ``end`` times
        aligned to the resolution. The start is inclusive and the end is
        exclusive. By default the range includes the current, incomplete bucket.
        ``limit`` specifies the range in time buckets and cannot be combined
        with ``start`` or ``range``.

        The response includes the requested time range and a ``series`` mapping.
        Each metric provides chronological data points, its unit, a summary and
        the time of its latest observation. Missing data is not treated as zero.
        Only saved observations are returned; results may be cached for ten
        seconds.

        Invalid requests return HTTP 422. Queries must stay within retention,
        1,000 time buckets and 100 matching metrics.
        """
        params = request.query_params
        try:
            result = await chancy.plugins["chancy.metrics"].get_metrics(
                chancy,
                metric_prefix=request.path_params["prefix"],
                worker_id=params.get("worker_id"),
                resolution=params.get("resolution", "5min"),
                start=datetime.fromisoformat(params["start"])
                if "start" in params
                else None,
                end=datetime.fromisoformat(params["end"])
                if "end" in params
                else None,
                range_seconds=int(params["range"])
                if "range" in params
                else None,
                limit=int(params["limit"]) if "limit" in params else None,
            )
        except (ValueError, OverflowError) as exc:
            return Response(
                json_dumps({"title": str(exc)}),
                media_type="application/json",
                status_code=422,
            )
        return Response(
            json_dumps(
                {
                    **result,
                    "series": {
                        key: {
                            **asdict(metric),
                            "aggregation": metric.aggregation,
                            "sampled_at": metric.sampled_at,
                            "summary": metric.summary,
                        }
                        for key, metric in result["series"].items()
                    },
                }
            ),
            media_type="application/json",
        )
