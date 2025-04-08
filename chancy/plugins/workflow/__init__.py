import asyncio
import enum
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from typing import List, Dict, TextIO, Self
from psycopg import sql, AsyncCursor
from psycopg.rows import dict_row

from chancy.hub import Event
from chancy.plugin import Plugin
from chancy.app import Chancy
from chancy.worker import Worker
from chancy.job import Job, QueuedJob, IsAJob, COMPLETED_STATES
from chancy.utils import json_dumps, chancy_uuid
from chancy.rule import Rule


@dataclass
class WorkflowStep:
    #: The job to execute when this step is ready.
    job: Job | IsAJob
    #: The unique ID of the step.
    step_id: str
    #: A list of step IDs that this step depends on.
    dependencies: List[str] = field(default_factory=list)
    #: The current state of the step.
    state: QueuedJob.State | None = QueuedJob.State.PENDING
    #: The unique ID of a running Job which is associated with this step.
    job_id: str | None = None


@dataclass
class Workflow:
    class State(enum.Enum):
        PENDING = "pending"
        RUNNING = "running"
        COMPLETED = "completed"
        FAILED = "failed"

    # A descriptive name for the workflow.
    name: str
    #: A dictionary of steps in the workflow, keyed by step ID.
    steps: Dict[str, WorkflowStep] = field(default_factory=dict)
    #: The current state of the workflow.
    state: State = State.PENDING

    #: The unique ID of a specific run of the workflow.
    id: str = field(default_factory=chancy_uuid)
    #: The time the workflow was created.
    created_at: datetime | None = None
    #: The time the workflow was last updated.
    updated_at: datetime | None = None

    def add(
        self, step_id: str, job: Job | IsAJob, dependencies: List[str] = None
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
        jobs: List[tuple[str, Job | IsAJob]],
        dependencies: List[str] = None,
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

    def __iadd__(self, other: WorkflowStep) -> "Workflow":
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

    @property
    def steps_by_state(self) -> Dict[QueuedJob.State, List[WorkflowStep]]:
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

    Enable the plugin by adding it to the list of plugins in the Chancy
    constructor:

    .. code-block:: python

        from chancy.plugins.leadership import Leadership
        from chancy.plugins.workflow import WorkflowPlugin

        async with Chancy(
            "postgresql://localhost/postgres",
            plugins=[Leadership(), WorkflowPlugin()]
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


    :param polling_interval: The interval at which to poll for new workflows.
    :param max_workflows_per_run: The maximum number of workflows to process
                                  in a single run of the plugin.
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
        pruning_rule: Rule = Rules.Age() > 60 * 60 * 24,
    ):
        super().__init__()
        self.polling_interval = polling_interval
        self.max_workflows_per_run = max_workflows_per_run
        self.pruning_rule = pruning_rule

    async def run(self, worker: Worker, chancy: Chancy):
        worker.hub.on(
            "workflow.created",
            lambda *args, **kwargs: self.wake_up(),
        )
        worker.hub.on(
            "workflow.step_completed",
            partial(
                self._on_single_step_completed, chancy=chancy, worker=worker
            ),
        )

        while await self.sleep(self.polling_interval):
            await self.wait_for_leader(worker)

            async with chancy.pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cursor:
                    # Grab the workflow IDs of workflows that are pending or
                    # running and lock them with FOR UPDATE.
                    await cursor.execute(
                        sql.SQL(
                            """
                            SELECT id
                            FROM {workflows} w
                            WHERE w.state IN ('pending', 'running')
                            FOR UPDATE SKIP LOCKED
                            LIMIT %(limit)s
                            """
                        ).format(
                            workflows=sql.Identifier(
                                f"{chancy.prefix}workflows"
                            ),
                        ),
                        {
                            "limit": self.max_workflows_per_run,
                        },
                    )

                    results = await cursor.fetchall()
                    if not results:
                        continue

                    workflows = await self.fetch_workflows_ex(
                        cursor,
                        chancy,
                        ids=[row["id"] for row in results],
                    )
                    for workflow in workflows:
                        if await self.process_workflow(
                            cursor, chancy, workflow
                        ):
                            await self.push_ex(cursor, chancy, workflow)

    async def _on_single_step_completed(
        self, event: Event, chancy: Chancy, worker: Worker
    ):
        """
        Called whenever a single step in a workflow is completed.

        Used to immediately process the workflow and check if it can be
        progressed to the next step without waiting for the next polling
        interval.
        """
        if not worker.is_leader:
            return

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT w.id
                        FROM {workflows} w
                        WHERE w.id = %(workflow_id)s
                        FOR UPDATE SKIP LOCKED
                        """
                    ).format(
                        workflows=sql.Identifier(f"{chancy.prefix}workflows"),
                        workflow_steps=sql.Identifier(
                            f"{chancy.prefix}workflow_steps"
                        ),
                    ),
                    {"workflow_id": event.body["workflow_id"]},
                )
                # Already locked by someone else.
                if not await cursor.fetchone():
                    return

                workflow = await self.fetch_workflow_ex(
                    cursor, chancy, event.body["workflow_id"]
                )
                if workflow is not None:
                    if await self.process_workflow(cursor, chancy, workflow):
                        await self.push_ex(cursor, chancy, workflow)

    async def on_job_updated(
        self,
        *,
        worker: "Worker",
        job: QueuedJob,
    ):
        if not job.meta.get("workflow_id"):
            return

        await worker.hub.emit(
            "workflow.step_completed", {"workflow_id": job.meta["workflow_id"]}
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
        cls, cursor: AsyncCursor, chancy: Chancy, id_: str
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
    ) -> List[Workflow]:
        """
        Fetch workflows from the database, optionally matching the given
        conditions.

        :param chancy: The Chancy application.
        :param states: A list of states to match.
        :param ids: A list of IDs to match.
        :param limit: The maximum number of workflows to fetch.
        :return: A list of workflows.
        """
        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                return await cls.fetch_workflows_ex(
                    cursor,
                    chancy,
                    states=states,
                    ids=ids,
                    limit=limit,
                )

    @staticmethod
    async def fetch_workflows_ex(
        cursor: AsyncCursor,
        chancy: Chancy,
        *,
        states: list[str] | None = None,
        ids: list[str] | None = None,
        limit: int = 100,
    ) -> List[Workflow]:
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
                        )
                    ), '[]'::json) as steps
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
                        job=Job.unpack(step["job_data"]),
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
        cursor: AsyncCursor, chancy: Chancy, workflow: Workflow
    ) -> bool:
        """
        Process a single iteration of the given workflow, progressing the
        state of each step and the overall workflow as necessary.

        :param cursor: The cursor to use for the query.
        :param chancy: The Chancy application.
        :param workflow: The workflow to process.
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
        for step_id, step in workflow.steps.items():
            # If the step is already in a terminal state, we can skip it.
            if step.state in COMPLETED_STATES:
                continue

            dependencies = [workflow.steps[dep] for dep in step.dependencies]
            if all(
                dep.state == QueuedJob.State.SUCCEEDED for dep in dependencies
            ):
                if step.job_id is None:
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

        # Are all jobs complete, or any jobs failed? If so, we can mark the
        # workflow as completed or failed.
        states = workflow.steps_by_state
        if len(states.get(QueuedJob.State.SUCCEEDED, [])) == len(workflow):
            workflow.state = Workflow.State.COMPLETED
        elif states.get(QueuedJob.State.FAILED):
            workflow.state = Workflow.State.FAILED
        elif states.get(QueuedJob.State.EXPIRED):
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
        async with chancy.pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor(row_factory=dict_row) as cursor:
                    return await cls.push_ex(cursor, chancy, workflow)

    @staticmethod
    async def push_ex(
        cursor: AsyncCursor, chancy: Chancy, workflow: Workflow
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
        :return: The UUID of the newly created workflow.
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
                    json_dumps(step.job.pack()),
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
            for dep in step.dependencies:
                output.write(f'  "{dep}" -> "{step_id}";\n')

        # Add workflow info
        output.write('  labelloc="t";\n')
        output.write(
            f'  label="Workflow: {workflow.name}\\nState: {workflow.state}";\n'
        )

        # Close the digraph
        output.write("}\n")

    async def cleanup(self, chancy: Chancy) -> int | None:
        async with chancy.pool.connection() as conn:
            async with conn.cursor() as cursor:
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
        timeout: float | int | None = None,
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

    def __init__(self, name: str, jobs: List[Job | IsAJob] = None):
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
