import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, TextIO
from psycopg import sql
from psycopg.rows import dict_row

from chancy.plugin import Plugin, PluginScope
from chancy.app import Chancy
from chancy.worker import Worker
from chancy.job import Job, JobInstance
from chancy.utils import json_dumps, chancy_uuid
from chancy.rule import Rule


@dataclass
class WorkflowStep:
    job: Job
    step_id: str
    dependencies: List[str] = field(default_factory=list)
    state: JobInstance.State | None = JobInstance.State.PENDING
    job_id: str | None = None


@dataclass
class Workflow:
    class State(enum.Enum):
        PENDING = "pending"
        RUNNING = "running"
        COMPLETED = "completed"
        FAILED = "failed"

    name: str
    steps: Dict[str, WorkflowStep] = field(default_factory=dict)
    state: State = State.PENDING

    id: str = field(default_factory=chancy_uuid)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def add_step(
        self, step_id: str, job: Job, dependencies: List[str] = None
    ) -> "Workflow":
        """
        Add a step to the workflow.

        :param step_id: The ID of the step.
        :param job: The job to execute.
        :param dependencies: A list of step IDs that this step depends on.
        """
        self.steps[step_id] = WorkflowStep(
            job=job, dependencies=dependencies or [], step_id=step_id
        )
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
    def steps_by_state(self) -> Dict[JobInstance.State, List[WorkflowStep]]:
        steps_by_state = {}
        for step in self.steps.values():
            steps_by_state.setdefault(step.state, []).append(step)
        return steps_by_state


class WorkflowPlugin(Plugin):
    """
    Support for simple dependency-based workflows.

    Workflows are defined by a series of steps, each of which can depend on
    one or more other steps. When all dependencies are met, the step is
    executed. This forms a directed acyclic graph (DAG) of steps that can be
    visualized as a workflow.

    If notifications are enabled (the default) workflows will run almost
    immediately after creation.

    You can use the :func:`generate_dot` method to generate a DOT file
    representation of a workflow, which can be visualized using Graphviz or
    similar tools. This can be a helpful debugging and validation tool for
    complex workflows.

    Events
    ------

    The WorkflowPlugin reacts to the following events:

    - ``workflow.created``: Triggered when a new workflow is created.

    Example
    -------

    .. code-block:: python
       :caption: example_workflow.py

        import asyncio
        from chancy import Job, Chancy
        from chancy.plugins.workflow import (
            Workflow,
            WorkflowStep,
            WorkflowPlugin
        )

        async def top():
            print(f"Top")

        async def left():
            print(f"Left")

        async def right():
            print(f"Right")

        async def bottom():
            print(f"Bottom")

        async def main():
            async with Chancy(dsn="postgresql://localhost/postgres") as chancy:
                workflow = Workflow("example")

                workflow += WorkflowStep(
                    job=Job.from_func(top),
                    step_id="top",
                )
                workflow += WorkflowStep(
                    job=Job.from_func(left),
                    step_id="left",
                    dependencies=["top"],
                )
                workflow += WorkflowStep(
                    job=Job.from_func(right),
                    step_id="right",
                    dependencies=["top"],
                )
                workflow += WorkflowStep(
                    job=Job.from_func(bottom),
                    step_id="bottom",
                    dependencies=["left", "right"],
                )

                await WorkflowPlugin.push(chancy, workflow)

        if __name__ == "__main__":
            asyncio.run(main())

    If we visualize our newly created workflow using :func:`generate_dot`, we
    get:

    .. graphviz:: _static/workflow.dot

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
        polling_interval: int = 1,
        max_workflows_per_run: int = 1000,
        pruning_rule: Rule = Rules.Age() > 60 * 60 * 24,
    ):
        super().__init__()
        self.polling_interval = polling_interval
        self.max_workflows_per_run = max_workflows_per_run
        self.pruning_rule = pruning_rule

    @classmethod
    def get_scope(cls) -> PluginScope:
        return PluginScope.WORKER

    async def run(self, worker: Worker, chancy: Chancy):
        worker.hub.on(
            "workflow.created",
            lambda *args, **kwargs: self.wake_up(),
        )

        while await self.sleep(self.polling_interval):
            await self.wait_for_leader(worker)
            workflows = await self.fetch_workflows(
                chancy,
                states=["pending", "running"],
                limit=self.max_workflows_per_run,
            )
            for workflow in workflows:
                await self.process_workflow(chancy, workflow)

    async def fetch_workflow(self, chancy: Chancy, id_: str) -> Workflow | None:
        """
        Fetch a single workflow from the database.

        :param chancy: The Chancy application.
        :param id_: The ID of the workflow to fetch.
        :return: The workflow, or None if it does not exist.
        """
        workflows = await self.fetch_workflows(chancy, ids=[id_])
        return workflows[0] if workflows else None

    async def fetch_workflows(
        self,
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
                        workflow_steps=sql.Identifier(
                            f"{chancy.prefix}workflow_steps"
                        ),
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
                                    JobInstance.State(step["state"])
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

    async def process_workflow(self, chancy: Chancy, workflow: Workflow):
        """
        Process a single iteration of the given workflow, progressing the
        state of each step and the overall workflow as necessary.

        :param chancy: The Chancy application.
        :param workflow: The workflow to process.
        """
        # If the workflow is already in a terminal state, there's no further
        # processing to do, although we may add future state handling here
        # for retries.
        if workflow.state in [Workflow.State.COMPLETED, Workflow.State.FAILED]:
            return

        # We check each step in the workflow to see:
        #    - If it has an associated job, and if so, what's the state of it?
        #    - If it has any dependencies, and if so, are they all completed?
        # If all dependencies are met, we can execute the job.
        for step_id, step in workflow.steps.items():
            # If the step is already in a terminal state, we can skip it.
            if step.state in [
                JobInstance.State.SUCCEEDED,
                JobInstance.State.FAILED,
            ]:
                continue

            dependencies = [workflow.steps[dep] for dep in step.dependencies]
            if all(
                dep.state == JobInstance.State.SUCCEEDED for dep in dependencies
            ):
                if step.job_id is None:
                    step.job_id = (await chancy.push(step.job)).identifier

        # Are all jobs complete, or any jobs failed? If so, we can mark the
        # workflow as completed or failed.
        states = workflow.steps_by_state
        if len(states.get(JobInstance.State.SUCCEEDED, [])) == len(workflow):
            workflow.state = Workflow.State.COMPLETED
        elif states.get(JobInstance.State.FAILED):
            workflow.state = Workflow.State.FAILED

        # We update the workflow in the database to reflect the new state
        await self.push(chancy, workflow)

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
                async with conn.cursor() as cursor:
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
                            RETURNING id, created_at, updated_at
                            """
                        ).format(
                            workflows=sql.Identifier(
                                f"{chancy.prefix}workflows"
                            )
                        ),
                        [workflow.id, workflow.name, workflow.state.value],
                    )
                    result = await cursor.fetchone()
                    workflow.id, workflow.created_at, workflow.updated_at = (
                        result
                    )

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
                        "workflow.upserted",
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
            JobInstance.State.PENDING: "lightblue",
            JobInstance.State.RUNNING: "yellow",
            JobInstance.State.SUCCEEDED: "lightgreen",
            JobInstance.State.FAILED: "lightpink",
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

    def migrate_package(self) -> str:
        return "chancy.plugins.workflow.migrations"

    def migrate_key(self) -> str:
        return "workflow"
