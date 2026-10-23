import asyncio
import enum
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from itertools import islice
from typing import Self, TextIO
from uuid import UUID

from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow, dict_row

from chancy.app import Chancy
from chancy.hub import Event
from chancy.job import IsAJob, Job, QueuedJob, SerializedJob
from chancy.plugin import Plugin
from chancy.rule import Rule, SQLAble
from chancy.utils import chancy_uuid, json_dumps
from chancy.worker import Worker


class CircularDependencyError(ValueError):
    """Raised when a circular dependency is detected in a workflow."""


class InvalidDependencyError(ValueError):
    """Raised when a dependency references a non-existent step."""


class EmptyWorkflowError(ValueError):
    """Raised when a workflow has no steps."""


def _dfs_detect_cycle(
    step_id: str,
    steps: dict[str, "WorkflowStep"],
    color: dict[str, int],
    path: list[str],
) -> None:
    """
    DFS helper to detect cycles in workflow dependency graph.

    :param step_id: Current step being visited.
    :param steps: Dictionary of all workflow steps.
    :param color: Color mapping (0=WHITE/unvisited, 1=GRAY/in-stack, 2=BLACK/processed).
    :param path: Current path in the recursion stack.
    :raises CircularDependencyError: If a cycle is detected.
    """
    color[step_id] = 1  # Mark as GRAY (in recursion stack)
    path.append(step_id)

    for dep_id in steps[step_id].dependencies:
        if color[dep_id] == 1:  # GRAY - cycle detected
            # Find where the cycle starts
            cycle_start = path.index(dep_id)
            cycle_path = " -> ".join(path[cycle_start:] + [dep_id])
            raise CircularDependencyError(
                f"Circular dependency detected: {cycle_path}"
            )
        elif color[dep_id] == 0:  # WHITE - unvisited
            _dfs_detect_cycle(dep_id, steps, color, path)

    path.pop()
    color[step_id] = 2  # Mark as BLACK (processed)


@dataclass
class WorkflowStep:
    #: The job to execute when this step is ready.
    job: Job | IsAJob
    #: The unique ID of the step.
    step_id: str
    #: A list of step IDs that this step depends on.
    dependencies: list[str] = field(default_factory=list)
    #: The current state of the step.
    state: QueuedJob.State | None = QueuedJob.State.PENDING
    #: The unique ID of a running Job which is associated with this step.
    job_id: str | None = None


@dataclass
class Workflow:
    """
    A collection of jobs with dependencies between steps.

    Define steps using :meth:`add` or :meth:`add_group`, then submit the
    workflow using :meth:`WorkflowPlugin.push`. Each step is queued once all
    of its dependencies have succeeded. Steps without dependencies can run
    in parallel.

    For jobs that should run one after another, use :class:`Sequence`.
    """

    class State(enum.Enum):
        PENDING = "pending"
        RUNNING = "running"
        COMPLETED = "completed"
        FAILED = "failed"

    # A descriptive name for the workflow.
    name: str
    #: A dictionary of steps in the workflow, keyed by step ID.
    steps: dict[str, WorkflowStep] = field(default_factory=dict)
    #: The current state of the workflow.
    state: State = State.PENDING

    #: The unique ID of a specific run of the workflow.
    id: str = field(default_factory=chancy_uuid)
    #: The time the workflow was created.
    created_at: datetime | None = None
    #: The time the workflow was last updated.
    updated_at: datetime | None = None

    def add(
        self,
        step_id: str,
        job: Job | IsAJob,
        dependencies: list[str] | None = None,
    ) -> "Workflow":
        """
        Add a step to the workflow.

        .. code-block:: python

            workflow.add("step_1", job)
            workflow.add("step_2", job, ["step_1"])

        :param step_id: The ID of the step.
        :param job: The job to execute.
        :param dependencies: A list of step IDs that this step depends on.
        """
        self.steps[step_id] = WorkflowStep(
            job=job if isinstance(job, Job) else job.job,
            dependencies=dependencies or [],
            step_id=step_id,
        )
        return self

    def add_group(
        self,
        jobs: list[tuple[str, Job | IsAJob]],
        dependencies: list[str] | None = None,
    ) -> "Workflow":
        """
        Add a group of steps to the workflow.

        This is a convenience method for adding multiple steps to the workflow
        at once that are all dependent on the same set of dependencies.

        .. code-block:: python

            workflow = Workflow("my_workflow")
            workflow.add("setup", setup_job)
            workflow.add_group([
                ("step_1", job_1),
                ("step_2", job_2),
                ("step_3", job_3),
            ], ["setup"])

        :param jobs: A list of tuples of (step_id, job).
        :param dependencies: A list of step IDs that this step depends on.
        """
        for step_id, job in jobs:
            self.add(step_id, job, dependencies)
        return self

    def __repr__(self):
        return f"<Workflow({self.name!r}, {self.state!r})>"

    def __len__(self) -> int:
        return len(self.steps)

    def __iadd__(self, other: WorkflowStep) -> Self:
        self.steps[other.step_id] = other
        return self

    def __iter__(self):
        return iter(self.steps.items())

    def __getitem__(self, key: str) -> WorkflowStep:
        return self.steps[key]

    def __delitem__(self, key: str):
        del self.steps[key]

    @property
    def is_complete(self) -> bool:
        return self.state in (self.State.COMPLETED, self.State.FAILED)

    @property
    def is_running(self) -> bool:
        return self.state == self.State.RUNNING

    def validate(self) -> None:
        """
        Validate the workflow's dependency graph.

        This method checks for:
        - Empty workflows (no steps)
        - Circular dependencies (cycles in the dependency graph)
        - Invalid dependencies (references to non-existent steps)

        :raises EmptyWorkflowError: If the workflow has no steps.
        :raises CircularDependencyError: If a circular dependency is detected.
        :raises InvalidDependencyError: If a dependency references a non-existent step.
        """
        if not self.steps:
            raise EmptyWorkflowError(
                "A workflow must contain at least one step"
            )

        # First, check that all dependencies reference existing steps
        for step_id, step in self.steps.items():
            for dep_id in step.dependencies:
                if dep_id not in self.steps:
                    raise InvalidDependencyError(
                        f"Step '{step_id}' depends on non-existent step '{dep_id}'"
                    )

        # Detect cycles using DFS with three-color marking
        # WHITE (0) = unvisited, GRAY (1) = in recursion stack, BLACK (2) = processed
        color = {step_id: 0 for step_id in self.steps}
        path: list[str] = []

        # Run DFS from all unvisited nodes
        for step_id in self.steps:
            if color[step_id] == 0:
                _dfs_detect_cycle(step_id, self.steps, color, path)

    @property
    def steps_by_state(self) -> dict[QueuedJob.State, list[WorkflowStep]]:
        steps_by_state = {}
        for step in self.steps.values():
            steps_by_state.setdefault(step.state, []).append(step)
        return steps_by_state


class WorkflowPlugin(Plugin):
    """
    Support for dependency-based workflows.

    Workflows allow you to easily model complex processes that involve
    multiple jobs, each of which may depend on the completion of one or more
    other jobs. Workflows are modeled as a directed acyclic graph (DAG)
    where each node represents a job and each edge represents a dependency
    between two jobs.

    Workflows are implemented on top of the existing Chancy job system, meaning
    you can use all the existing job features, such as job retries, timeouts,
    scheduling, and so on.

    .. note::
        This plugin is enabled by default, you only need to provide it in the
        list of plugins to customize its arguments or if ``no_default_plugins``
        is set to ``True``.

    Enable the plugin by adding it to the list of plugins in the Chancy
    constructor:

    .. code-block:: python

        from chancy.plugins.leadership import Leadership
        from chancy.plugins.workflow import WorkflowPlugin

        async with Chancy(
            "postgresql://localhost/postgres",
            plugins=[WorkflowPlugin()],
        ) as chancy:
            ...

    Example
    -------

    We'll create a simple workflow runs the "top" job first, then the "left"
    and "right" jobs in parallel, and finally the "bottom" job:

    .. code-block:: python
       :caption: example_workflow.py

        import asyncio
        from chancy import Chancy, job
        from chancy.plugins.leadership import Leadership
        from chancy.plugins.workflow import Workflow, WorkflowPlugin

        @job()
        async def top():
            print(f"Top")

        @job()
        async def left():
            print(f"Left")

        @job()
        async def right():
            print(f"Right")

        @job()
        async def bottom():
            print(f"Bottom")

        async def main():
            async with Chancy(
                "postgresql://localhost/postgres",
                plugins=[Leadership(), WorkflowPlugin()]
            ) as chancy:
                workflow = (
                    Workflow("example")
                    .add("top", top)
                    .add("left", left, ["top"])
                    .add("right", right, ["top"])
                    .add("bottom", bottom, ["left", "right"])
                )
                await WorkflowPlugin.push(chancy, workflow)

        if __name__ == "__main__":
            asyncio.run(main())

    If we visualize our newly created workflow using :func:`generate_dot`, we
    get:

    .. graphviz:: _static/workflow.dot

    The full Workflow API is a little verbose if you just want to run a series
    of jobs in a specific order. In that case, you can use the
    :class:`Sequence` class to create a workflow from a list of jobs.

    Django Integration
    ------------------

    This plugin can be made available to the Django ORM and Admin interface.

    To enable this, you need to add the following to your Django settings:

    .. code-block:: python

        INSTALLED_APPS = [
            ...,
            "chancy.plugins.workflow.django",
        ]

    You can then query the workflows and steps using the Django ORM:

    .. code-block:: python

        from chancy.plugins.workflow.django.models import (
            Workflow,
            WorkflowStep
        )

        workflow = Workflow.objects.get(id="...")

        completed_steps = WorkflowStep.objects.filter(
            workflow=workflow,
            state=QueuedJob.State.SUCCEEDED
        )


    :param polling_interval: The interval between recovery polls. Notification
                             traffic does not reset this interval.
    :param max_workflows_per_run: The maximum number of workflows to process
                                  in one polling or notification batch.
                                  Recovery polls continue where the previous
                                  batch left off.
    :param max_pending_workflows: The maximum number of distinct workflows
                                 awaiting event-driven processing. Further
                                 notifications rely on periodic polling.
    """

    class Rules:
        class Age(Rule):
            def __init__(self):
                super().__init__("age")

            def to_sql(self) -> sql.Composable:
                return sql.SQL("EXTRACT(EPOCH FROM (NOW() - created_at))")

    def __init__(
        self,
        *,
        polling_interval: int = 30,
        max_workflows_per_run: int = 1000,
        pruning_rule: SQLAble | None = None,
        max_pending_workflows: int = 10000,
    ):
        super().__init__()
        self.polling_interval = polling_interval
        self.max_workflows_per_run = max_workflows_per_run
        self.max_pending_workflows = max_pending_workflows
        self.pruning_rule = (
            pruning_rule
            if pruning_rule is not None
            else self.Rules.Age() > 60 * 60 * 24
        )
        # Progress and a fixed upper bound for the current polling sweep.
        self._last_polled_id: UUID | None = None
        self._poll_until_id: UUID | None = None
        # An insertion-ordered set: repeated events retain their place in line.
        self._pending_workflows: dict[UUID, None] = {}

    async def run(self, worker: Worker, chancy: Chancy):
        handler = partial(self._on_workflow_event, worker=worker)
        events = ("workflow.created", "workflow.step_completed")
        for event in events:
            worker.hub.on(event, handler)

        loop = asyncio.get_running_loop()
        next_poll = loop.time() + self.polling_interval
        retry = worker.database_retry("workflow scheduler")
        try:
            while True:
                await self.wait_for_leader(worker)
                self.wakeup_signal.clear()

                try:
                    # Event traffic never moves this deadline. Check it between
                    # batches so notifications cannot starve recovery polling.
                    if loop.time() >= next_poll:
                        await worker.increment_counter("workflows:poll_runs", 1)
                        changes = []
                        async with (
                            chancy.pool.connection() as conn,
                            conn.cursor(row_factory=dict_row) as cursor,
                        ):
                            await self.poll(
                                worker, chancy, cursor, changes=changes
                            )
                        await self._record_transitions(worker, changes)
                        retry.reset()
                        next_poll = loop.time() + self.polling_interval

                    if not worker.is_leader.is_set():
                        continue

                    if (
                        self._pending_workflows
                        and self.max_workflows_per_run > 0
                    ):
                        changes = []
                        async with (
                            chancy.pool.connection() as conn,
                            conn.cursor(row_factory=dict_row) as cursor,
                        ):
                            await self.process_pending(
                                worker, chancy, cursor, changes=changes
                            )
                        await self._record_transitions(worker, changes)
                        retry.reset()
                        if self._pending_workflows:
                            continue
                except retry.errors as exc:
                    # Hints may have been consumed and the transaction's commit
                    # may be uncertain. Recover from a fresh sweep after backoff.
                    self._last_polled_id = self._poll_until_id = None
                    next_poll = 0
                    await retry.wait(exc)
                    continue

                await self.sleep(max(0, next_poll - loop.time()))
        finally:
            for event in events:
                worker.hub.remove(event, handler)

    def _on_workflow_event(self, event: Event, worker: Worker):
        """Queue a hint without doing database work in the event listener."""
        if not worker.is_leader.is_set():
            return
        workflow_id = event.body.get("workflow_id", event.body.get("id"))
        try:
            workflow_id = UUID(str(workflow_id))
        except ValueError:
            return
        if len(self._pending_workflows) < self.max_pending_workflows:
            self._pending_workflows.setdefault(workflow_id, None)
            self.wake_up()

    async def process_pending(
        self,
        worker: Worker,
        chancy: Chancy,
        cursor: AsyncCursor[DictRow],
        *,
        changes: list | None = None,
    ) -> int:
        """
        Process one batch of notified workflows using the caller's transaction.

        Duplicate notifications are coalesced until their batch starts. Locked
        workflows and dropped hints are recovered by periodic polling.

        :return: The number of workflows processed.
        """
        ids = list(islice(self._pending_workflows, self.max_workflows_per_run))
        if not ids:
            return 0
        # Remove hints before the first await. A completion arriving while a
        # batch is in flight must be retained for the next batch.
        for workflow_id in ids:
            del self._pending_workflows[workflow_id]

        await cursor.execute(
            sql.SQL(
                """
                SELECT id FROM {workflows}
                WHERE id = ANY(%(ids)s::uuid[])
                    AND state IN ('pending', 'running')
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                """
            ).format(workflows=sql.Identifier(f"{chancy.prefix}workflows")),
            {"ids": ids},
        )
        results = await cursor.fetchall()
        return await self._process_workflows(
            cursor,
            chancy,
            worker,
            [row["id"] for row in results],
            changes=changes,
        )

    async def _process_workflows(
        self,
        cursor: AsyncCursor[DictRow],
        chancy: Chancy,
        worker: Worker,
        ids: list[UUID],
        *,
        changes: list | None = None,
    ) -> int:
        """Load and advance workflows already locked by the caller."""
        if not ids:
            return 0
        workflows = await self.fetch_workflows_ex(
            cursor, chancy, ids=ids, limit=len(ids)
        )
        for workflow in workflows:
            previous = workflow.state
            queued_before = sum(
                step.job_id is not None for step in workflow.steps.values()
            )
            if await self.process_workflow(cursor, chancy, workflow, worker):
                if workflow.steps:
                    await self.push_ex(cursor, chancy, workflow, worker)
                else:
                    # Persist the failure of a legacy empty workflow without
                    # allowing empty definitions through submission validation.
                    await self._persist_workflow(
                        cursor, chancy, workflow, worker
                    )
                if changes is not None:
                    queued = (
                        sum(
                            step.job_id is not None
                            for step in workflow.steps.values()
                        )
                        - queued_before
                    )
                    changes.append((previous, workflow, queued))
        return len(workflows)

    @staticmethod
    async def _record_transitions(worker: Worker, changes: list):
        """Publish scheduler observations only after the batch commits."""
        for previous, workflow, queued in changes:
            if queued:
                await worker.increment_counter("workflows:steps:queued", queued)
            if previous == workflow.state:
                continue
            state = workflow.state.value
            await worker.increment_counter(f"workflows:state:{state}", 1)
            label = (
                "started" if workflow.state == Workflow.State.RUNNING else state
            )
            await worker.increment_counter(
                f"workflow:{workflow.name}:{label}", 1
            )
            if (
                workflow.state
                in (Workflow.State.COMPLETED, Workflow.State.FAILED)
                and workflow.created_at
                and workflow.updated_at
            ):
                duration = (
                    workflow.updated_at - workflow.created_at
                ).total_seconds()
                for key in (
                    "workflows:execution_time",
                    f"workflow:{workflow.name}:execution_time",
                ):
                    await worker.record_histogram_value(
                        key, duration, unit="seconds"
                    )

    async def poll(
        self,
        worker: Worker,
        chancy: Chancy,
        cursor: AsyncCursor[DictRow],
        *,
        changes: list | None = None,
    ) -> int:
        """
        Process the next batch of up to ``max_workflows_per_run`` pending or
        running workflows.

        Workflows are taken in ID order within a sweep whose upper bound is
        fixed at its start. Newer workflows join the next sweep, so arrivals
        cannot indefinitely postpone revisiting older workflows. A batch
        stops at the end of its sweep even if it has room for more workflows.

        :param worker: The worker processing the workflows.
        :param chancy: The Chancy application.
        :param cursor: The cursor to use for the query.
        :return: The number of workflows processed.
        """
        table = sql.Identifier(f"{chancy.prefix}workflows")
        after = self._last_polled_id
        until = self._poll_until_id
        # If the tail of a sweep has finished or is locked, try a fresh sweep
        # in this poll. Two attempts suffice, including when no work exists.
        for _ in range(2):
            if until is None:
                # The partial index supplies the sweep boundary without
                # scanning retained terminal workflows.
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT id FROM {workflows}
                        WHERE state IN ('pending', 'running')
                        ORDER BY id DESC LIMIT 1
                        """
                    ).format(workflows=table)
                )
                last = await cursor.fetchone()
                if last is None:
                    self._last_polled_id = self._poll_until_id = None
                    return 0
                until = last["id"]

            await cursor.execute(
                sql.SQL(
                    """
                    SELECT id
                    FROM {workflows} w
                    WHERE w.state IN ('pending', 'running')
                        AND w.id <= %(until)s::uuid
                        {after_filter}
                    ORDER BY w.id
                    LIMIT %(limit)s
                    FOR UPDATE SKIP LOCKED
                    """
                ).format(
                    workflows=table,
                    after_filter=(
                        sql.SQL("AND w.id > %(after)s::uuid")
                        if after is not None
                        else sql.SQL("")
                    ),
                ),
                {
                    "after": after,
                    "until": until,
                    "limit": self.max_workflows_per_run,
                },
            )
            results = await cursor.fetchall()
            if results:
                break
            if after is None:
                self._last_polled_id = self._poll_until_id = None
                return 0
            after = until = None

        processed = await self._process_workflows(
            cursor,
            chancy,
            worker,
            [row["id"] for row in results],
            changes=changes,
        )

        if (
            len(results) < self.max_workflows_per_run
            or results[-1]["id"] == until
        ):
            self._last_polled_id = self._poll_until_id = None
        else:
            self._last_polled_id = results[-1]["id"]
            self._poll_until_id = until

        return processed

    async def on_jobs_updated_in_transaction(
        self,
        *,
        worker: "Worker",
        jobs: Sequence[QueuedJob],
        cursor: AsyncCursor[DictRow],
    ):
        workflow_ids = dict.fromkeys(
            job.meta["workflow_id"]
            for job in jobs
            if job.state in (QueuedJob.State.SUCCEEDED, QueuedJob.State.FAILED)
            and job.meta.get("workflow_id")
        )
        await worker.chancy.notify_many(
            cursor,
            (
                ("workflow.step_completed", {"workflow_id": workflow_id})
                for workflow_id in workflow_ids
            ),
        )

    @classmethod
    async def fetch_workflow(cls, chancy: Chancy, id_: str) -> Workflow | None:
        """
        Fetch a single workflow from the database.

        :param chancy: The Chancy application.
        :param id_: The ID of the workflow to fetch.
        :return: The workflow, or None if it does not exist.
        """
        workflows = await cls.fetch_workflows(chancy, ids=[id_])
        return workflows[0] if workflows else None

    @classmethod
    async def fetch_workflow_ex(
        cls, cursor: AsyncCursor[DictRow], chancy: Chancy, id_: str
    ) -> Workflow | None:
        """
        Fetch a single workflow from the database.

        This method is a lower-level version of fetch_workflow that accepts
        an existing cursor object, allowing it to be used in transactions.

        :param cursor: The cursor to use for the query.
        :param chancy: The Chancy application.
        :param id_: The ID of the workflow to fetch.
        :return: The workflow, or None if it does not exist.
        """
        workflows = await cls.fetch_workflows_ex(cursor, chancy, ids=[id_])
        return workflows[0] if workflows else None

    @classmethod
    async def fetch_workflows(
        cls,
        chancy: Chancy,
        *,
        states: list[str] | None = None,
        ids: list[str] | None = None,
        limit: int = 100,
    ) -> list[Workflow]:
        """
        Fetch workflows from the database, optionally matching the given
        conditions.

        :param chancy: The Chancy application.
        :param states: A list of states to match.
        :param ids: A list of IDs to match.
        :param limit: The maximum number of workflows to fetch.
        :return: A list of workflows.
        """
        async with (
            chancy.pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cursor,
        ):
            return await cls.fetch_workflows_ex(
                cursor,
                chancy,
                states=states,
                ids=ids,
                limit=limit,
            )

    @staticmethod
    async def fetch_workflows_ex(
        cursor: AsyncCursor[DictRow],
        chancy: Chancy,
        *,
        states: list[str] | None = None,
        ids: list[str] | None = None,
        limit: int = 100,
    ) -> list[Workflow]:
        """
        Fetch workflows from the database, optionally matching the given
        conditions.

        This method is a lower-level version of fetch_workflows that accepts
        an existing cursor object, allowing it to be used in transactions.

        :param cursor: The cursor to use for the query.
        :param chancy: The Chancy application.
        :param states: A list of states to match.
        :param ids: A list of IDs to match.
        :param limit: The maximum number of workflows to fetch.
        :return: A list of workflows.
        """
        await cursor.execute(
            sql.SQL(
                """
                SELECT 
                    w.id, 
                    w.name, 
                    w.state,
                    w.created_at,
                    w.updated_at,
                    COALESCE(json_agg(
                        json_build_object(
                            'step_id', ws.step_id,
                            'job_data', ws.job_data,
                            'dependencies', ws.dependencies,
                            'state', j.state,
                            'job_id', ws.job_id
                        ) order by ws.step_id
                    ) FILTER (WHERE ws.step_id IS NOT NULL), '[]'::json) as steps
                FROM {workflows} w
                LEFT JOIN {workflow_steps} ws ON w.id = ws.workflow_id
                LEFT JOIN {jobs} j ON ws.job_id = j.id
                WHERE (
                    %(states)s::text[] IS NULL OR
                    w.state = ANY(%(states)s::text[])
                ) AND (
                    %(ids)s::uuid[] IS NULL OR
                    w.id = ANY(%(ids)s::uuid[])
                )
                GROUP BY w.id, w.name, w.state
                LIMIT {limit}
                """
            ).format(
                workflows=sql.Identifier(f"{chancy.prefix}workflows"),
                workflow_steps=sql.Identifier(f"{chancy.prefix}workflow_steps"),
                jobs=sql.Identifier(f"{chancy.prefix}jobs"),
                limit=sql.Literal(limit),
            ),
            {
                "states": states,
                "ids": ids,
            },
        )
        rows = await cursor.fetchall()

        return [
            Workflow(
                id=row["id"],
                name=row["name"],
                state=Workflow.State(row["state"]),
                updated_at=row["updated_at"],
                created_at=row["created_at"],
                steps={
                    step["step_id"]: WorkflowStep(
                        job=SerializedJob.unpack(step["job_data"]),
                        dependencies=step["dependencies"],
                        state=(
                            QueuedJob.State(step["state"])
                            if step["state"]
                            else None
                        ),
                        step_id=step["step_id"],
                        job_id=step["job_id"],
                    )
                    for step in row["steps"]
                },
            )
            for row in rows
        ]

    @staticmethod
    async def process_workflow(
        cursor: AsyncCursor,
        chancy: Chancy,
        workflow: Workflow,
        worker: "Worker",
    ) -> bool:
        """
        Process a single iteration of the given workflow, progressing the
        state of each step and the overall workflow as necessary.

        :param cursor: The cursor to use for the query.
        :param chancy: The Chancy application.
        :param workflow: The workflow to process.
        :param worker: The worker processing the workflow.
        :return: True if the workflow was updated, False otherwise.
        """
        # If the workflow is already in a terminal state, there's no further
        # processing to do, although we may add future state handling here
        # for retries.
        if workflow.state in [Workflow.State.COMPLETED, Workflow.State.FAILED]:
            return False

        # Keep track of whether any changes were made to the workflow. If no
        # changes are made, we can skip updating the database.
        has_change = False
        starting_state = workflow.state

        # We check each step in the workflow to see:
        #    - If it has an associated job, and if so, what's the state of it?
        #    - If it has any dependencies, and if so, are they all completed?
        # If all dependencies are met, we can execute the job.
        for step in workflow.steps.values():
            # If the step is already in a terminal state, we can skip it.
            if step.state in [
                QueuedJob.State.SUCCEEDED,
                QueuedJob.State.FAILED,
            ]:
                continue

            dependencies = [workflow.steps[dep] for dep in step.dependencies]
            if (
                all(
                    dep.state == QueuedJob.State.SUCCEEDED
                    for dep in dependencies
                )
                and step.job_id is None
            ):
                step.job_id = (
                    await chancy.push_ex(
                        cursor,
                        step.job.with_meta(
                            {
                                **step.job.meta,
                                "workflow_id": str(workflow.id),
                            }
                        ),
                    )
                ).identifier
                has_change = True

        # Transition from PENDING to RUNNING once any step has been queued
        if workflow.state == Workflow.State.PENDING and any(
            step.job_id is not None for step in workflow.steps.values()
        ):
            workflow.state = Workflow.State.RUNNING

        # Are all jobs complete, or any jobs failed? If so, we can mark the
        # workflow as completed or failed.
        states = workflow.steps_by_state
        if workflow.steps and len(
            states.get(QueuedJob.State.SUCCEEDED, [])
        ) == len(workflow):
            workflow.state = Workflow.State.COMPLETED
        elif not workflow.steps or states.get(QueuedJob.State.FAILED):
            workflow.state = Workflow.State.FAILED

        return starting_state != workflow.state or has_change

    @classmethod
    async def push(cls, chancy: Chancy, workflow: Workflow) -> str:
        """
        Push new workflow to the database.

        If the workflow already exists in the database, it will be updated
        instead.

        :param chancy: The Chancy application.
        :param workflow: The workflow to push.
        :return: The UUID of the newly created workflow.
        """
        async with (
            chancy.pool.connection() as conn,
            conn.transaction(),
            conn.cursor(row_factory=dict_row) as cursor,
        ):
            return await cls.push_ex(cursor, chancy, workflow)

    @classmethod
    async def push_ex(
        cls,
        cursor: AsyncCursor[DictRow],
        chancy: Chancy,
        workflow: Workflow,
        worker: "Worker" = None,
    ) -> str:
        """
        Push new workflow to the database.

        This method is a lower-level version of push that accepts an existing
        cursor object, allowing it to be used in transactions.

        If the workflow already exists in the database, it will be updated
        instead.

        :param cursor: The cursor to use for the query.
        :param chancy: The Chancy application.
        :param workflow: The workflow to push.
        :param worker: Associated worker. The scheduler emits metrics after commit.
        :return: The UUID of the newly created workflow.
        :raises EmptyWorkflowError: If the workflow has no steps.
        :raises CircularDependencyError: If a circular dependency is detected.
        :raises InvalidDependencyError: If a dependency references a non-existent step.

        Each step's job is validated before anything is stored.
        """
        workflow.validate()
        jobs = {
            step_id: chancy.serialize(step.job)
            for step_id, step in workflow.steps.items()
        }
        return await cls._persist_workflow(
            cursor, chancy, workflow, worker, jobs=jobs
        )

    @staticmethod
    async def _persist_workflow(
        cursor: AsyncCursor[DictRow],
        chancy: Chancy,
        workflow: Workflow,
        worker: "Worker" = None,
        *,
        jobs: dict[str, Job] | None = None,
    ) -> str:
        """
        Persist a validated submission or a scheduler state transition, with
        ``jobs`` in place of the steps' jobs when given.
        """
        await cursor.execute(
            sql.SQL(
                """
                INSERT INTO {workflows} (
                    id,
                    name,
                    state,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, NOW(), NOW())
                ON CONFLICT (id) DO UPDATE
                SET name = EXCLUDED.name,
                    state = EXCLUDED.state,
                    updated_at = NOW()
                RETURNING
                    id,
                    created_at,
                    updated_at,
                    (xmax = 0) as inserted
                """
            ).format(workflows=sql.Identifier(f"{chancy.prefix}workflows")),
            [workflow.id, workflow.name, workflow.state.value],
        )
        result = await cursor.fetchone()

        workflow.id = result["id"]
        workflow.created_at = result["created_at"]
        workflow.updated_at = result["updated_at"]
        inserted = result["inserted"]

        for step_id, step in workflow.steps.items():
            await cursor.execute(
                sql.SQL(
                    """
                    INSERT INTO {workflow_steps} (
                        workflow_id,
                        step_id,
                        job_data,
                        dependencies,
                        job_id
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (workflow_id, step_id) DO UPDATE
                    SET job_data = EXCLUDED.job_data,
                        dependencies = EXCLUDED.dependencies,
                        job_id = EXCLUDED.job_id,
                        updated_at = NOW()
                    """
                ).format(
                    workflow_steps=sql.Identifier(
                        f"{chancy.prefix}workflow_steps"
                    )
                ),
                [
                    workflow.id,
                    step_id,
                    json_dumps(
                        (jobs[step_id] if jobs is not None else step.job).pack()
                    ),
                    json_dumps(step.dependencies),
                    step.job_id,
                ],
            )

        await chancy.notify(
            cursor,
            f"workflow.{'created' if inserted else 'updated'}",
            {
                "id": workflow.id,
                "name": workflow.name,
            },
        )

        return workflow.id

    @staticmethod
    def generate_dot(workflow: Workflow, output: TextIO):
        """
        Generate a DOT file representation of the workflow.

        :param workflow: The Workflow object to visualize.
        :param output: A file-like object to write the DOT content to.
        """
        # Start the digraph
        output.write(f'digraph "{workflow.name}" {{\n')
        output.write("  rankdir=TB;\n")
        output.write(
            '  node [shape=box, style="rounded,filled", fontname="Arial"];\n'
        )

        # Define color scheme
        colors = {
            QueuedJob.State.PENDING: "lightblue",
            QueuedJob.State.RUNNING: "yellow",
            QueuedJob.State.SUCCEEDED: "lightgreen",
            QueuedJob.State.FAILED: "lightpink",
        }

        # Add nodes (steps)
        for step_id, step in workflow.steps.items():
            color = colors.get(step.state, "lightgray")
            output.write(
                f'  "{step_id}" [label="{step_id}\\n({step.state})",'
                f" fillcolor={color}];\n"
            )

        # Add edges (dependencies)
        for step_id, step in workflow.steps.items():
            output.writelines(
                f'  "{dep}" -> "{step_id}";\n' for dep in step.dependencies
            )

        # Add workflow info
        output.write('  labelloc="t";\n')
        output.write(
            f'  label="Workflow: {workflow.name}\\nState: {workflow.state}";\n'
        )

        # Close the digraph
        output.write("}\n")

    async def cleanup(self, chancy: Chancy) -> int | None:
        async with chancy.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                sql.SQL(
                    """
                        DELETE FROM {workflows}
                        WHERE state NOT IN ('pending', 'running')
                        AND ({rule})
                        """
                ).format(
                    workflows=sql.Identifier(f"{chancy.prefix}workflows"),
                    rule=self.pruning_rule.to_sql(),
                )
            )
            return cursor.rowcount

    @classmethod
    async def wait_for_workflow(
        cls,
        chancy: Chancy,
        workflow_id: str,
        *,
        interval: int = 1,
        timeout: float | None = None,
    ) -> Workflow:
        """
        Wait for a workflow to complete.

        This method will loop until the workflow referenced by the provided ID
        has completed. The interval parameter controls how often the workflow
        status is checked. This will not block the event loop, so other tasks
        can run while waiting for the workflow to complete.

        Example
        -------

        .. code-block:: python

            workflow = Workflow("example")
            workflow.add("step1", job1)
            workflow.add("step2", job2, ["step1"])

            workflow_id = await WorkflowPlugin.push(chancy, workflow)
            completed_workflow = await WorkflowPlugin.wait_for_workflow(
                chancy,
                workflow_id,
                timeout=300  # 5 minute timeout
            )

        :param chancy: The Chancy application.
        :param workflow_id: The ID of the workflow to wait for.
        :param interval: The number of seconds to wait between checks.
        :param timeout: The maximum number of seconds to wait for the workflow to
            complete. If not provided, the method will wait indefinitely.
        :raises asyncio.TimeoutError: If the timeout is reached before the workflow
            completes.
        :raises KeyError: If the workflow does not exist.
        :return: The completed Workflow object.
        """
        async with asyncio.timeout(timeout):
            while True:
                workflow = await cls.fetch_workflow(chancy, workflow_id)
                if workflow is None:
                    raise KeyError(f"Workflow {workflow_id} not found")

                if workflow.is_complete:
                    return workflow

                await asyncio.sleep(interval)

    def migrate_package(self) -> str:
        return "chancy.plugins.workflow.migrations"

    def migrate_key(self) -> str:
        return "workflow"

    def api_plugin(self) -> str | None:
        return "chancy.plugins.workflow.api.WorkflowApiPlugin"

    def get_tables(self) -> list[str]:
        """Get the names of all tables this plugin is responsible for."""
        return ["workflows", "workflow_steps"]

    @staticmethod
    def get_identifier() -> str:
        return "chancy.workflow_plugin"

    @staticmethod
    def get_dependencies() -> list[str]:
        return ["chancy.leadership"]


class Sequence:
    """
    A sequential workflow.

    Sequences are a special case of workflows, where each step depends on the
    previous step. This forms a linear chain of jobs that are executed in
    order.

    Sequences are useful for defining sequences of jobs that must be executed
    in order, without the complexity of full workflows.

    Example
    -------

    .. code-block:: python
       :caption: example_sequence.py

        import asyncio
        from chancy import Chancy, job
        from chancy.plugins.workflow import Sequence

        @job()
        async def first():
            print("First")

        @job()
        async def second():
            print("Second")

        @job()
        async def third():
            print("Third")

        async def main():
            async with Chancy("postgresql://localhost/postgres") as chancy:
                sequence = Sequence("example_workflow", [first, second, third])
                await sequence.push(chancy)

        if __name__ == "__main__":
            asyncio.run(main())
    """

    def __init__(self, name: str, jobs: list[Job | IsAJob] | None = None):
        self.name = name
        self.jobs = jobs or []

    def add(self, job: Job | IsAJob) -> Self:
        """
        Add a job to the sequence.

        .. code-block:: python

            workflow = (
                Sequence("example_sequence")
                .add(first)
                .add(second)
                .add(third)
            )

        :param job: The job to add.
        """
        self.jobs.append(job)
        return self

    async def push(self, chancy: Chancy) -> str:
        """
        Push a sequence to the database.

        :param chancy: The Chancy application.
        :return: The UUID of the newly created chain.
        """
        workflow = Workflow(self.name)
        for i, job in enumerate(self.jobs):
            step_id = f"step_{i}"
            dependencies = [f"step_{i - 1}"] if i > 0 else []
            workflow.add(step_id, job, dependencies)

        return await WorkflowPlugin.push(chancy, workflow)
