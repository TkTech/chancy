from dataclasses import dataclass, field
from typing import List, Dict, Any, TextIO
from psycopg import sql
from psycopg.rows import dict_row

from chancy.plugin import Plugin, PluginScope
from chancy.app import Chancy
from chancy.worker import Worker
from chancy.job import Job, Reference
from chancy.utils import json_dumps
from chancy.rule import Rule


@dataclass
class WorkflowStep:
    job: Job
    dependencies: List[str] = field(default_factory=list)
    state: str = "pending"
    step_id: str | None = None
    job_id: str | None = None


@dataclass
class Workflow:
    name: str
    steps: Dict[str, WorkflowStep] = field(default_factory=dict)
    id: int | None = None
    state: str = "pending"

    def add_step(
        self, step_id: str, job: Job, dependencies: List[str] = None
    ) -> "Workflow":
        self.steps[step_id] = WorkflowStep(
            job=job, dependencies=dependencies or [], step_id=step_id
        )
        return self

    def __repr__(self):
        return f"<Workflow({self.name!r}, {self.state!r})>"

    def __iadd__(self, other: WorkflowStep) -> "Workflow":
        self.steps[other.step_id] = other
        return self


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
                chancy, states=["pending", "running"]
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
    ) -> List[Workflow]:
        """
        Fetch workflows from the database, optionally matching the given
        conditions.

        :param chancy: The Chancy application.
        :param states: A list of states to match.
        :param ids: A list of IDs to match.
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
                            COALESCE(json_agg(
                                json_build_object(
                                    'step_id', ws.step_id,
                                    'job_data', ws.job_data,
                                    'dependencies', ws.dependencies,
                                    'state', ws.state,
                                    'job_id', ws.job_id
                                )
                            ) FILTER (
                                WHERE ws.step_id IS NOT NULL
                            ), '[]'::json) as steps
                        FROM {workflows} w
                        LEFT JOIN {workflow_steps} ws ON w.id = ws.workflow_id
                        WHERE (
                            %(states)s::text[] IS NULL OR
                            w.state = ANY(%(states)s::text[])
                        ) AND (
                            %(ids)s::integer[] IS NULL OR
                            w.id = ANY(%(ids)s::integer[])
                        )
                        GROUP BY w.id, w.name, w.state
                        LIMIT {limit}
                        """
                    ).format(
                        workflows=sql.Identifier(f"{chancy.prefix}workflows"),
                        workflow_steps=sql.Identifier(
                            f"{chancy.prefix}workflow_steps"
                        ),
                        limit=sql.Literal(self.max_workflows_per_run),
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
                        state=row["state"],
                        steps={
                            step["step_id"]: WorkflowStep(
                                job=Job.unpack(step["job_data"]),
                                dependencies=step["dependencies"],
                                state=step["state"],
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
        Process a single iteration of the given workflow.

        :param chancy: The Chancy application.
        :param workflow: The workflow to process.
        """
        workflow_updated = False
        steps_to_start = []
        steps_to_check = []

        # Identify steps to start and check
        for step in workflow.steps.values():
            if step.state == "pending" and all(
                workflow.steps[dep].state == "succeeded"
                for dep in step.dependencies
                if dep in workflow.steps
            ):
                steps_to_start.append(step)
            elif step.state == "running":
                steps_to_check.append(step)

        # Start pending steps
        for step in steps_to_start:
            ref = await chancy.push(step.job)
            step.job_id = ref.identifier
            step.state = "running"
            await self.update_workflow_step(chancy, workflow.id, step)
            workflow_updated = True

        # Check running steps
        for step in steps_to_check:
            job_instance = await chancy.get_job(Reference(chancy, step.job_id))
            if job_instance.state in ("succeeded", "failed"):
                step.state = job_instance.state
                await self.update_workflow_step(chancy, workflow.id, step)
                workflow_updated = True

        # Update workflow state if needed
        if workflow_updated:
            if workflow.state == "pending" and any(
                step.state == "running" for step in workflow.steps.values()
            ):
                workflow.state = "running"
                await self.update_workflow_state(chancy, workflow)
            elif all(
                step.state in ("succeeded", "failed")
                for step in workflow.steps.values()
            ):
                workflow.state = "completed"
                await self.update_workflow_state(chancy, workflow)

    @classmethod
    async def update_workflow_step(
        cls, chancy: Chancy, workflow_id: int, step: WorkflowStep
    ):
        """
        Update a workflow step in the database.

        .. note::

            This currently only updates the state, job_id, and updated_at,
            as other fields are not expected to change.

        :param chancy: The Chancy application.
        :param workflow_id: The ID of the workflow.
        :param step: The step to update.
        """
        async with chancy.pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        UPDATE {workflow_steps}
                        SET state = %s, job_id = %s, updated_at = NOW()
                        WHERE workflow_id = %s AND step_id = %s
                        """
                    ).format(
                        workflow_steps=sql.Identifier(
                            f"{chancy.prefix}workflow_steps"
                        )
                    ),
                    [step.state, step.job_id, workflow_id, step.step_id],
                )

    @classmethod
    async def update_workflow_state(cls, chancy: Chancy, workflow: Workflow):
        """
        Update a workflow in the database.

        .. note::

            This currently only updates the state and updated_at, as other
            fields are not expected to change.

        :param chancy: The Chancy application.
        :param workflow: The workflow to update.
        """
        async with chancy.pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        UPDATE {workflows}
                        SET state = %s, updated_at = NOW()
                        WHERE id = %s
                        """
                    ).format(
                        workflows=sql.Identifier(f"{chancy.prefix}workflows")
                    ),
                    [workflow.state, workflow.id],
                )

    @classmethod
    async def push(cls, chancy: Chancy, workflow: Workflow) -> int:
        """
        Push a new workflow to the database.

        :param chancy: The Chancy application.
        :param workflow: The workflow to push.
        :return: The ID of the newly created workflow.
        """
        async with chancy.pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        sql.SQL(
                            """
                            INSERT INTO {workflows} (name, state)
                            VALUES (%s, %s)
                            RETURNING id
                            """
                        ).format(
                            workflows=sql.Identifier(
                                f"{chancy.prefix}workflows"
                            )
                        ),
                        [workflow.name, workflow.state],
                    )
                    workflow_id = await cursor.fetchone()
                    workflow.id = workflow_id[0]

                    for step_id, step in workflow.steps.items():
                        await cursor.execute(
                            sql.SQL(
                                """
                                INSERT INTO {workflow_steps} (
                                    workflow_id,
                                    step_id,
                                    job_data,
                                    dependencies,
                                    state
                                )
                                VALUES (%s, %s, %s, %s, %s)
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
                                step.state,
                            ],
                        )

                    await chancy.notify(
                        cursor,
                        "workflow.created",
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
            "pending": "lightblue",
            "running": "yellow",
            "succeeded": "lightgreen",
            "failed": "lightpink",
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
        output.write(f'  labelloc="t";\n')
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
                        DELETE FROM {workflow_steps}
                        WHERE workflow_id NOT IN (
                            SELECT id FROM {workflows}
                        )
                        """
                    ).format(
                        workflow_steps=sql.Identifier(
                            f"{chancy.prefix}workflow_steps"
                        ),
                        workflows=sql.Identifier(f"{chancy.prefix}workflows"),
                    )
                )
                steps_removed = cursor.rowcount

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
                workflows_removed = cursor.rowcount

                return steps_removed + workflows_removed

    def migrate_package(self) -> str:
        return "chancy.plugins.workflow.migrations"

    def migrate_key(self) -> str:
        return "workflow"
