from psycopg import sql
from psycopg.rows import dict_row
import asyncio
import json
import time
import importlib.metadata
from starlette.authentication import requires
from starlette.requests import Request
from starlette.responses import Response
from starlette.websockets import WebSocket, WebSocketDisconnect

from chancy.plugins.api.plugin import ApiPlugin
from chancy.queue import Queue
from chancy.job import Reference
from chancy.rule import JobRules
from chancy.utils import json_dumps


# Simple in-memory cache for expensive queries
_functions_cache = {"data": None, "timestamp": 0}
_FUNCTIONS_CACHE_TTL = 60  # 60 seconds


def parse_filters(filters_param, field_config):
    """
    Parse filter triples and build a rule from them.

    Args:
        filters_param: JSON string of filter triples [[key, op, value], ...]
        field_config: Dict mapping field keys to (Rule, [allowed_operators])

    Returns:
        tuple: (rule, error_response)
        - rule: Combined Rule object or None
        - error_response: Response object if error, None if success
    """
    if not filters_param:
        return None, None

    try:
        filters = json.loads(filters_param)
        if not isinstance(filters, list):
            return None, Response(
                json_dumps({"title": "filters must be an array"}),
                media_type="application/json",
                status_code=422,
            )

        rule = None
        for filter_triple in filters:
            if not isinstance(filter_triple, list) or len(filter_triple) != 3:
                return None, Response(
                    json_dumps(
                        {"title": "Each filter must be [key, operator, value]"}
                    ),
                    media_type="application/json",
                    status_code=422,
                )

            key, operator, value = filter_triple

            if key not in field_config:
                return None, Response(
                    json_dumps({"title": f"Unknown filter field: {key}"}),
                    media_type="application/json",
                    status_code=422,
                )

            rule_obj, allowed_ops = field_config[key]

            if operator not in allowed_ops:
                return None, Response(
                    json_dumps(
                        {
                            "title": f"Operator '{operator}' not allowed for field '{key}'"
                        }
                    ),
                    media_type="application/json",
                    status_code=422,
                )

            # Apply the filter based on operator
            filter_condition = None
            if operator == "=":
                filter_condition = rule_obj == value
            elif operator == "~":
                filter_condition = rule_obj.ilike(f"%{value}%")
            elif operator == ">":
                filter_condition = rule_obj > value
            elif operator == "<":
                filter_condition = rule_obj < value
            elif operator == ">=":
                filter_condition = rule_obj >= value
            elif operator == "<=":
                filter_condition = rule_obj <= value

            if filter_condition:
                if rule:
                    rule &= filter_condition
                else:
                    rule = filter_condition

        return rule, None

    except json.JSONDecodeError:
        return None, Response(
            json_dumps({"title": "Invalid JSON in filters parameter"}),
            media_type="application/json",
            status_code=422,
        )


class CoreApiPlugin(ApiPlugin):
    """
    Core API plugin which implements the endpoints for jobs, queues, workers,
    etc...
    """

    def name(self):
        return "base"

    def routes(self):
        return [
            {
                "path": "/health",
                "endpoint": self.health,
                "methods": ["GET"],
                "name": "health",
            },
            {
                "path": "/api/v1/configuration",
                "endpoint": self.get_configuration,
                "methods": ["GET"],
                "name": "get_configuration",
            },
            {
                "path": "/api/v1/ws",
                "endpoint": self.ws_events,
                "is_websocket": True,
                "name": "ws_events",
            },
            {
                "path": "/api/v1/login",
                "endpoint": self.login,
                "methods": ["POST"],
                "name": "login",
            },
            {
                "path": "/api/v1/logout",
                "endpoint": self.logout,
                "methods": ["POST"],
                "name": "logout",
            },
            {
                "path": "/api/v1/queues",
                "endpoint": self.queues,
                "methods": ["GET", "POST"],
                "name": "queues",
            },
            {
                "path": "/api/v1/workers",
                "endpoint": self.get_workers,
                "methods": ["GET"],
                "name": "get_workers",
            },
            {
                "path": "/api/v1/jobs",
                "endpoint": self.jobs,
                "methods": ["GET", "POST"],
                "name": "jobs",
            },
            {
                "path": "/api/v1/jobs/functions",
                "endpoint": self.jobs_functions,
                "methods": ["GET"],
                "name": "jobs_functions",
            },
            {
                "path": "/api/v1/jobs/{id}",
                "endpoint": self.job,
                "methods": ["GET", "DELETE"],
                "name": "job",
            },
            {
                "path": "/api/v1/jobs/{id}/retry",
                "endpoint": self.retry_job,
                "methods": ["POST"],
                "name": "retry_job",
            },
            {
                "path": "/api/v1/jobs/{id}/cancel",
                "endpoint": self.cancel_job,
                "methods": ["POST"],
                "name": "cancel_job",
            },
            {
                "path": "/api/v1/queues/{name}",
                "endpoint": self.queue,
                "methods": ["PATCH", "DELETE"],
                "name": "queue",
            },
            {
                "path": "/api/v1/queues/{name}/pause",
                "endpoint": self.pause_queue,
                "methods": ["POST"],
                "name": "pause_queue",
            },
            {
                "path": "/api/v1/queues/{name}/resume",
                "endpoint": self.resume_queue,
                "methods": ["POST"],
                "name": "resume_queue",
            },
            {
                "path": "/api/v1/system",
                "endpoint": self.system_info,
                "methods": ["GET"],
                "name": "system_info",
            },
            {
                "path": "/api/v1/plugins",
                "endpoint": self.plugins,
                "methods": ["GET"],
                "name": "plugins",
            },
        ]

    @staticmethod
    async def health(request: Request, *, chancy, worker):
        """
        Health check endpoint - returns 200 if the worker is healthy.
        Does not require authentication.
        """
        return Response(
            json_dumps({"status": "healthy"}),
            media_type="application/json",
            status_code=200,
        )

    @staticmethod
    @requires(["authenticated"])
    async def system_info(request: Request, *, chancy, worker):
        """
        Get system information including version, plugins, and database status.
        """
        version = importlib.metadata.version("chancy")

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                # Get database version
                await cursor.execute("SELECT version()")
                db_version = await cursor.fetchone()

        return Response(
            json_dumps(
                {
                    "chancy_version": version,
                    "database": {
                        "version": db_version["version"]
                        if db_version
                        else None,
                        "prefix": chancy.prefix,
                    },
                }
            ),
            media_type="application/json",
        )

    @staticmethod
    @requires(["authenticated"])
    async def plugins(request: Request, *, chancy, worker):
        """
        Get detailed information about all registered plugins.
        """
        plugins_info = []
        for plugin in chancy.plugins.values():
            plugin_data = {
                "identifier": plugin.get_identifier(),
                "tables": plugin.get_tables(),
                "migrate_key": plugin.migrate_key(),
                "migrate_package": plugin.migrate_package(),
                "api_plugin": plugin.api_plugin(),
                "dependencies": plugin.get_dependencies(),
                "scope": plugin.get_scope().value,
            }
            plugins_info.append(plugin_data)

        return Response(
            json_dumps(plugins_info),
            media_type="application/json",
        )

    async def login(self, request: Request, *, chancy, worker):
        """
        Login endpoint.
        """
        data = await request.json()

        username = data.get("username")
        password = data.get("password")

        if not username or not password:
            return Response(
                json_dumps({"error": "Invalid username or password"}),
                status_code=401,
                media_type="application/json",
            )

        if not await self.api.authentication_backend.login(
            request, username, password
        ):
            return Response(
                json_dumps({"error": "Invalid username or password"}),
                status_code=401,
                media_type="application/json",
            )

        # Issue a signed bearer token for stateless auth.
        from itsdangerous import URLSafeTimedSerializer

        serializer = URLSafeTimedSerializer(
            self.api.secret_key, salt="chancy.api.token"
        )
        token = serializer.dumps({"u": username})

        return Response(
            json_dumps({"token": token}),
            media_type="application/json",
        )

    async def logout(self, request: Request, *, chancy, worker):
        """
        Logout endpoint.
        """
        return Response(
            json_dumps({"success": True}), media_type="application/json"
        )

    async def ws_events(self, websocket: WebSocket, *, chancy, worker):
        """
        WebSocket stream of core system events.
        """
        # Authenticate via token in query param
        token = websocket.query_params.get("token")
        if not token:
            await websocket.close(code=1008)
            return
        from itsdangerous import URLSafeTimedSerializer, BadSignature

        try:
            URLSafeTimedSerializer(
                self.api.secret_key, salt="chancy.api.token"
            ).loads(token, max_age=60 * 60 * 24 * 7)
        except BadSignature:
            await websocket.close(code=1008)
            return

        await websocket.accept()

        queue: asyncio.Queue[tuple[str, dict]] = asyncio.Queue(maxsize=256)
        alive = True

        async def handler(event):
            nonlocal alive
            if not alive:
                return
            try:
                queue.put_nowait((event.name, event.body))
            except asyncio.QueueFull:
                # Drop if client is slow
                pass

        # Stream all hub events for gossip/debugging purposes
        worker.hub.on_any(handler)

        try:
            while True:
                event, body = await queue.get()
                await websocket.send_text(
                    json_dumps({"event": event, "data": body})
                )
        except WebSocketDisconnect:
            pass
        finally:
            alive = False
            worker.hub.remove_on_any(handler)

    @staticmethod
    @requires(["authenticated"])
    async def get_configuration(request, *, chancy, worker):
        """
        Get the configuration of the Chancy instance.
        """
        return Response(
            json_dumps(
                {
                    "plugins": [
                        plugin.__class__.__name__
                        for plugin in chancy.plugins.values()
                    ]
                }
            ),
            media_type="application/json",
        )

    @staticmethod
    @requires(["authenticated"])
    async def queues(request: Request, *, chancy, worker):
        """
        GET: Get a list of all queues.
        POST: Create a queue.
        """
        if request.method == "GET":
            queues = await chancy.get_all_queues()
            return Response(
                json_dumps([queue.pack() for queue in queues]),
                media_type="application/json",
            )

        data = await request.json()
        try:
            q = Queue(
                name=data["name"],
                concurrency=data.get("concurrency"),
                tags=set(data.get("tags") or []),
                state=Queue.State(data.get("state", "active")),
                executor=data.get("executor")
                or "chancy.executors.process.ProcessExecutor",
                executor_options=data.get("executor_options") or {},
                polling_interval=data.get("polling_interval", 5),
                rate_limit=data.get("rate_limit"),
                rate_limit_window=data.get("rate_limit_window"),
                resume_at=data.get("resume_at"),
                eager_polling=data.get("eager_polling", False),
            )
        except Exception as e:
            return Response(
                json_dumps(
                    {"title": "Invalid queue payload", "detail": str(e)}
                ),
                media_type="application/json",
                status_code=422,
            )

        q = await chancy.declare(q, upsert=True)
        return Response(json_dumps(q.pack()), media_type="application/json")

    @staticmethod
    @requires(["authenticated"])
    async def get_workers(request, *, chancy, worker):
        """
        Get a list of all the workers.
        """
        workers = await chancy.get_all_workers()
        return Response(
            json_dumps(workers),
            media_type="application/json",
        )

    @staticmethod
    @requires(["authenticated"])
    async def jobs_functions(request: Request, *, chancy, worker):
        """
        GET: Get a list of distinct function names from jobs.
        Uses in-memory caching to avoid expensive DISTINCT queries.
        """
        global _functions_cache

        # Check if cache is valid
        current_time = time.time()
        if (
            _functions_cache["data"] is not None
            and current_time - _functions_cache["timestamp"]
            < _FUNCTIONS_CACHE_TTL
        ):
            return Response(
                json_dumps(_functions_cache["data"]),
                media_type="application/json",
            )

        # Cache miss or expired, fetch from database
        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT DISTINCT func FROM {jobs}
                        WHERE func IS NOT NULL
                        ORDER BY func
                        """
                    ).format(
                        jobs=sql.Identifier(f"{chancy.prefix}jobs"),
                    )
                )
                result = await cursor.fetchall()
                functions = [row["func"] for row in result]

                # Update cache
                _functions_cache["data"] = functions
                _functions_cache["timestamp"] = current_time

                return Response(
                    json_dumps(functions),
                    media_type="application/json",
                )

    @staticmethod
    @requires(["authenticated"])
    async def jobs(request: Request, *, chancy, worker):
        """
        GET: Get a list of jobs with filters and pagination.
        POST: Batch job actions (retry, purge).
        """
        if request.method == "POST":
            data = await request.json()
            action = (data.get("action") or "").lower()
            ids = data.get("ids") or []
            if not ids:
                return Response(
                    json_dumps({"title": "No job IDs provided"}),
                    media_type="application/json",
                    status_code=422,
                )
            refs = [Reference(j) for j in ids]
            if action == "retry":
                await chancy.retry_jobs(refs)
            elif action == "purge":
                await chancy.purge_jobs(refs)
            else:
                return Response(
                    json_dumps({"title": "Invalid batch action"}),
                    media_type="application/json",
                    status_code=422,
                )
            return Response(
                json_dumps({"ok": True}), media_type="application/json"
            )

        # Support both legacy query params and new filter triples
        state = request.query_params.get("state")
        queue = request.query_params.get("queue")
        func = request.query_params.get("func")
        limit = min(int(request.query_params.get("limit", 100)), 100)
        before = request.query_params.get("before")

        # New filter triples: filters=[["key","op","value"],...]
        filters_param = request.query_params.get("filters")

        rule = None

        if state:
            rule = JobRules.State() == state

        if before:
            if rule:
                rule &= JobRules.ID() < before
            else:
                rule = JobRules.ID() < before

        # Legacy parameters for backward compatibility
        if queue:
            if rule:
                rule &= JobRules.Queue() == queue
            else:
                rule = JobRules.Queue() == queue

        if func:
            if rule:
                rule &= JobRules.Job().contains(func)
            else:
                rule = JobRules.Job().contains(func)

        # Process filter triples using helper
        field_config = {
            "state": (JobRules.State(), ["="]),
            "queue": (JobRules.Queue(), ["="]),
            "func": (JobRules.Job(), ["=", "~"]),
            "priority": (JobRules.Priority(), ["=", ">", "<", ">=", "<="]),
            "attempts": (JobRules.Attempts(), ["=", ">", "<", ">=", "<="]),
        }

        filter_rule, error = parse_filters(filters_param, field_config)
        if error:
            return error

        if filter_rule:
            if rule:
                rule &= filter_rule
            else:
                rule = filter_rule

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                if rule:
                    query = sql.SQL(
                        """
                        SELECT * FROM {jobs}
                        WHERE ({rule})
                        ORDER BY (
                            completed_at,
                            scheduled_at
                        ) DESC
                        LIMIT {limit}
                        """
                    ).format(
                        jobs=sql.Identifier(f"{chancy.prefix}jobs"),
                        rule=rule.to_sql(),
                        limit=sql.Literal(limit),
                    )
                else:
                    query = sql.SQL(
                        """
                        SELECT * FROM {jobs}
                        ORDER BY (
                            completed_at,
                            scheduled_at
                        ) DESC
                        LIMIT {limit}
                        """
                    ).format(
                        jobs=sql.Identifier(f"{chancy.prefix}jobs"),
                        limit=sql.Literal(limit),
                    )

                await cursor.execute(query)

                result = await cursor.fetchall()
                return Response(
                    json_dumps(result or []),
                    media_type="application/json",
                )

    @staticmethod
    @requires(["authenticated"])
    async def job(request, *, chancy, worker):
        """
        GET: Get a single job by ID.
        DELETE: Purge a job by ID.
        """
        job_id = request.path_params["id"]

        if request.method == "DELETE":
            await chancy.purge_jobs([Reference(job_id)])
            return Response(
                json_dumps({"ok": True}), media_type="application/json"
            )

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT * FROM {jobs}
                        WHERE id = %(job_id)s
                        """
                    ).format(
                        jobs=sql.Identifier(f"{chancy.prefix}jobs"),
                    ),
                    {"job_id": job_id},
                )

                result = await cursor.fetchone()

                if not result:
                    return Response(
                        json_dumps({}),
                        status_code=404,
                        media_type="application/json",
                    )

                return Response(
                    json_dumps(result),
                    media_type="application/json",
                )

    @staticmethod
    @requires(["authenticated"])
    async def retry_job(request: Request, *, chancy, worker):
        job_id = request.path_params["id"]
        await chancy.retry_jobs([Reference(job_id)])
        return Response(json_dumps({"ok": True}), media_type="application/json")

    @staticmethod
    @requires(["authenticated"])
    async def cancel_job(request: Request, *, chancy, worker):
        job_id = request.path_params["id"]
        await chancy.cancel_job(Reference(job_id))
        return Response(json_dumps({"ok": True}), media_type="application/json")

    @staticmethod
    @requires(["authenticated"])
    async def queue(request: Request, *, chancy, worker):
        name = request.path_params["name"]

        if request.method == "DELETE":
            purge_jobs = request.query_params.get(
                "purge_jobs", "true"
            ).lower() in (
                "true",
                "1",
                "yes",
            )
            await chancy.delete_queue(name, purge_jobs=purge_jobs)
            return Response(
                json_dumps({"ok": True}), media_type="application/json"
            )

        # PATCH
        data = await request.json()
        try:
            existing = await chancy.get_queue(name)
        except KeyError:
            return Response(
                json_dumps({"title": "Queue not found"}),
                media_type="application/json",
                status_code=404,
            )

        q = Queue(
            name=name,
            concurrency=data.get("concurrency", existing.concurrency),
            tags=set(data.get("tags", list(existing.tags))),
            state=Queue.State(data.get("state", existing.state.value)),
            executor=data.get("executor", existing.executor),
            executor_options=data.get(
                "executor_options", existing.executor_options
            ),
            polling_interval=data.get(
                "polling_interval", existing.polling_interval
            ),
            rate_limit=data.get("rate_limit", existing.rate_limit),
            rate_limit_window=data.get(
                "rate_limit_window", existing.rate_limit_window
            ),
            resume_at=data.get("resume_at", existing.resume_at),
            eager_polling=data.get("eager_polling", existing.eager_polling),
        )
        q = await chancy.declare(q, upsert=True)
        return Response(json_dumps(q.pack()), media_type="application/json")

    @staticmethod
    @requires(["authenticated"])
    async def pause_queue(request: Request, *, chancy, worker):
        name = request.path_params["name"]
        data = await request.json()
        resume_at = data.get("resume_at")
        await chancy.pause_queue(name, resume_at=resume_at)
        return Response(json_dumps({"ok": True}), media_type="application/json")

    @staticmethod
    @requires(["authenticated"])
    async def resume_queue(request: Request, *, chancy, worker):
        name = request.path_params["name"]
        await chancy.resume_queue(name)
        return Response(json_dumps({"ok": True}), media_type="application/json")
