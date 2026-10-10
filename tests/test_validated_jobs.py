import asyncio
import dataclasses
import sys
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
import pytest_asyncio
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Json

from chancy import (
    Chancy,
    Job,
    Queue,
    QueuedJob,
    Reference,
    SerializedJob,
    Worker,
)
from chancy.executors.thread import ThreadedExecutor
from chancy.plugins.cron import Cron
from chancy.plugins.leadership import ImmediateLeadership
from chancy.plugins.trigger import Trigger
from chancy.plugins.workflow import Workflow, WorkflowPlugin
from chancy.utils import chancy_uuid, importable_name
from chancy.validation import (
    JSON,
    JobValidationError,
    ValidationSkippedWarning,
    Validator,
)


@dataclasses.dataclass
class Greeting:
    user_id: uuid.UUID
    name: str
    times: int = 1


class GreetingValidator(Validator):
    def __init__(self, *, max_times: int):
        self.max_times = max_times

    def supports(self, annotation: object) -> bool:
        return annotation is Greeting

    def check(self, greeting: Greeting):
        if greeting.times > self.max_times:
            raise ValueError(f"times must be at most {self.max_times}")

    def load(self, annotation: object, value: JSON) -> Greeting:
        if not isinstance(value, dict):
            raise TypeError(f"expected an object, got {value!r}")
        greeting = Greeting(
            user_id=uuid.UUID(str(value["user_id"])),
            name=str(value["name"]),
            times=int(str(value.get("times", 1))),
        )
        self.check(greeting)
        return greeting

    def dump(self, annotation: object, value: Any) -> JSON:
        if not isinstance(value, Greeting):
            raise TypeError(f"expected Greeting, got {type(value).__name__}")
        self.check(value)
        return {
            "user_id": str(value.user_id),
            "name": value.name,
            "times": value.times,
        }


def greet(greeting: Greeting, punctuation: str = "", *, context: QueuedJob):
    context.meta["greeting"] = (
        f"hello {greeting.name}{punctuation}" * greeting.times
    )


async def async_greet(greeting: Greeting, *, context: QueuedJob):
    context.meta["greeting"] = f"hello {greeting.name}" * greeting.times


greet_job = Job.from_func(greet)


def with_greeting_validator(*plugins):
    return pytest.mark.parametrize(
        "chancy",
        [
            {
                "validators": [GreetingValidator(max_times=3)],
                "plugins": list(plugins),
                "no_default_plugins": True,
            }
        ],
        indirect=True,
    )


async def count_rows(chancy: Chancy, table: str) -> int:
    async with (
        chancy.pool.connection() as conn,
        conn.cursor() as cursor,
    ):
        await cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {table}").format(
                table=sql.Identifier(f"{chancy.prefix}{table}")
            )
        )
        (count,) = await cursor.fetchone()
        return count


async def insert_job_directly(
    chancy: Chancy,
    kwargs: dict,
    *,
    max_attempts: int = 1,
    meta: dict | None = None,
) -> Reference:
    job_id = chancy_uuid()
    async with (
        chancy.pool.connection() as conn,
        conn.cursor() as cursor,
    ):
        await cursor.execute(
            sql.SQL(
                """
                INSERT INTO {jobs} (id, queue, func, kwargs, max_attempts, meta)
                VALUES (%s, 'default', %s, %s, %s, %s)
                """
            ).format(jobs=sql.Identifier(f"{chancy.prefix}jobs")),
            (
                job_id,
                importable_name(greet),
                Json(kwargs),
                max_attempts,
                Json(meta or {}),
            ),
        )
    return Reference(job_id)


@pytest_asyncio.fixture
async def users_table(chancy: Chancy, test_suffix: str):
    table_name = f"validated_users{test_suffix}"
    async with chancy.pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {table} (id SERIAL PRIMARY KEY)"
            ).format(table=sql.Identifier(table_name))
        )

    yield table_name

    async with chancy.pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {table} CASCADE").format(
                table=sql.Identifier(table_name)
            )
        )


@with_greeting_validator()
@pytest.mark.asyncio
async def test_job_receives_loaded_value(
    chancy: Chancy, worker: Worker, sync_job_executor: str
):
    """
    A kwarg whose parameter annotation a validator supports goes
    through the configured validator when the job runs, on every executor. Other
    kwargs are passed through and the job context is still injected.
    """
    await chancy.declare(Queue("low", executor=sync_job_executor))

    ref = await chancy.push(
        greet_job.with_queue("low").with_kwargs(
            greeting=Greeting(user_id=uuid.uuid4(), name="Ada"),
            punctuation="!",
        )
    )
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.SUCCEEDED
    assert j.meta["greeting"] == "hello Ada!"


@with_greeting_validator()
@pytest.mark.asyncio
async def test_async_job_receives_loaded_value(
    chancy: Chancy, worker: Worker, async_job_executor: str
):
    """
    Asynchronous jobs receive the loaded value on every executor that
    supports them.
    """
    await chancy.declare(Queue("low", executor=async_job_executor))

    ref = await chancy.push(
        Job.from_func(async_greet, queue="low").with_kwargs(
            greeting=Greeting(user_id=uuid.uuid4(), name="Ada", times=2)
        )
    )
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.SUCCEEDED
    assert j.meta["greeting"] == "hello Adahello Ada"


@with_greeting_validator()
@pytest.mark.asyncio
async def test_push_rejects_invalid_kwargs(chancy: Chancy):
    """
    A kwarg the validator rejects fails the push and nothing reaches the
    queue, not even the valid jobs pushed alongside.
    """
    await chancy.declare(Queue("default"))

    with pytest.raises(JobValidationError, match="times must be at most 3"):
        async for _ in chancy.push_many(
            [
                greet_job.with_kwargs(
                    greeting=Greeting(user_id=uuid.uuid4(), name="Ada")
                ),
                greet_job.with_kwargs(
                    greeting=Greeting(user_id=uuid.uuid4(), name="Ada", times=9)
                ),
            ]
        ):
            pass

    assert await count_rows(chancy, "jobs") == 0


@with_greeting_validator()
@pytest.mark.asyncio
async def test_sync_push_rejects_plain_json(chancy: Chancy):
    """
    A parameter annotated with a type a validator supports takes an object of
    that type: plain JSON pushed for it is refused, with the synchronous push
    too, and nothing is stored.
    """
    with chancy:
        chancy.sync_declare(Queue("default"))
        with pytest.raises(JobValidationError, match="expected Greeting"):
            chancy.sync_push(
                greet_job.with_kwargs(
                    greeting={"user_id": str(uuid.uuid4()), "name": "Ada"}
                )
            )

    assert await count_rows(chancy, "jobs") == 0


@with_greeting_validator()
@pytest.mark.asyncio
async def test_push_stores_dumped_kwargs(chancy: Chancy):
    """
    Validated kwargs are stored in the form returned by the validator, defaults
    included, and other kwargs are stored as given.
    """
    await chancy.declare(Queue("default"))
    user_id = uuid.uuid4()

    ref = await chancy.push(
        greet_job.with_kwargs(
            greeting=Greeting(user_id=user_id, name="Ada"), punctuation="!"
        )
    )
    j = await chancy.get_job(ref)

    assert j.kwargs == {
        "greeting": {"user_id": str(user_id), "name": "Ada", "times": 1},
        "punctuation": "!",
    }


@with_greeting_validator()
@pytest.mark.asyncio
async def test_job_created_by_name_is_validated_when_pushed(chancy: Chancy):
    """
    A job created from its function's name rather than the function itself
    is validated when pushed too, since the function is imported by name, so
    its annotated kwargs must be objects as well.
    """
    await chancy.declare(Queue("default"))

    with pytest.raises(JobValidationError, match="expected Greeting"):
        await chancy.push(
            Job(
                func=importable_name(greet),
                kwargs={
                    "greeting": {"user_id": str(uuid.uuid4()), "name": "A"}
                },
            )
        )

    assert await count_rows(chancy, "jobs") == 0


@with_greeting_validator()
@pytest.mark.asyncio
async def test_job_with_an_unknown_function_is_pushed_with_a_warning(
    chancy: Chancy,
):
    """
    A job whose function can't be found in the pushing process, like one
    pushed by name from a service without its code, is pushed with a warning
    saying it will only be validated when it runs.
    """
    await chancy.declare(Queue("default"))

    with pytest.warns(ValidationSkippedWarning, match="elsewhere.greet"):
        ref = await chancy.push(
            Job(
                func="elsewhere.greet",
                kwargs={"greeting": {"user_id": "nope", "name": "Ada"}},
            )
        )

    j = await chancy.get_job(ref)
    assert j.kwargs == {"greeting": {"user_id": "nope", "name": "Ada"}}


class ShoutingGreetingValidator(GreetingValidator):
    def load(self, annotation: object, value: Any) -> Greeting:
        greeting = super().load(annotation, value)
        return dataclasses.replace(greeting, name=greeting.name.upper())


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "validators": [
                ShoutingGreetingValidator(max_times=3),
                GreetingValidator(max_times=3),
            ],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_first_supporting_validator_is_used(
    chancy: Chancy, worker: Worker
):
    """
    When several validators support an annotation, the first one in the
    list given to Chancy validates it.
    """
    await chancy.declare(Queue("default"))

    ref = await chancy.push(
        greet_job.with_kwargs(
            greeting=Greeting(user_id=uuid.uuid4(), name="Ada")
        )
    )
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.SUCCEEDED
    assert j.meta["greeting"] == "hello ADA"


@with_greeting_validator()
@pytest.mark.asyncio
async def test_serialized_job_is_pushed_as_it_is(
    chancy: Chancy, worker: Worker
):
    """
    A job whose kwargs are already serialized, as the CLI pushes them, is
    pushed without going through the validators and validated when it runs.
    """
    await chancy.declare(Queue("default"))

    ref = await chancy.push(
        SerializedJob(
            func=importable_name(greet),
            kwargs={"greeting": {"user_id": str(uuid.uuid4()), "times": 9}},
        )
    )
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.FAILED
    assert "greeting" not in j.meta
    assert "JobValidationError" in j.errors[-1]["traceback"]


@with_greeting_validator()
@pytest.mark.asyncio
async def test_job_read_back_can_be_pushed_again(
    chancy: Chancy, worker: Worker
):
    """
    A job read back from the queue has its kwargs serialized, so it can be
    pushed again as it is.
    """
    await chancy.declare(Queue("default"))
    ref = await chancy.push(
        greet_job.with_kwargs(
            greeting=Greeting(user_id=uuid.uuid4(), name="Ada")
        )
    )
    first = await chancy.wait_for_job(ref, timeout=30)

    again = await chancy.wait_for_job(await chancy.push(first), timeout=30)

    assert again.state == again.State.SUCCEEDED
    assert again.meta["greeting"] == "hello Ada"


@with_greeting_validator()
@pytest.mark.asyncio
async def test_invalid_kwargs_inserted_elsewhere_fail_the_job(
    chancy: Chancy, worker: Worker
):
    """
    A job inserted without going through push, by SQL or another producer,
    is still validated when it runs, with the validator's configuration, and
    fails instead of calling the function with bad data.
    """
    await chancy.declare(Queue("default"))

    ref = await insert_job_directly(
        chancy,
        {"greeting": {"user_id": str(uuid.uuid4()), "name": "Ada", "times": 9}},
    )
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.FAILED
    assert "greeting" not in j.meta
    assert "times must be at most 3" in j.errors[-1]["traceback"]


INJECTED_GREETING = Greeting(user_id=uuid.uuid4(), name="Injected")


class GreetingInjectingExecutor(ThreadedExecutor):
    @classmethod
    def get_function_and_kwargs(cls, job):
        func, kwargs = super().get_function_and_kwargs(job)
        return func, {**kwargs, "greeting": INJECTED_GREETING}


def test_value_injected_over_a_stored_kwarg_is_not_loaded():
    """
    A value an executor injects in place of a stored kwarg reaches the
    function as it is, instead of being replaced by the loaded stored value.
    """
    job = QueuedJob(
        func=greet_job.func,
        id=uuid.uuid4(),
        created_at=datetime.now(tz=UTC),
        kwargs={"greeting": {"user_id": str(uuid.uuid4()), "name": "Ada"}},
    )._with_validators((GreetingValidator(max_times=3),))

    _, _, kwargs = GreetingInjectingExecutor.prepare_job_for_execution(job)

    assert kwargs["greeting"] is INJECTED_GREETING


@with_greeting_validator()
@pytest.mark.asyncio
async def test_serialized_job_can_be_pushed(chancy: Chancy, worker: Worker):
    """
    Serializing a job returns it in the form it is stored, which can be
    pushed or serialized again as it is.
    """
    await chancy.declare(Queue("default"))
    serialized = chancy.serialize(
        greet_job.with_kwargs(
            greeting=Greeting(user_id=uuid.uuid4(), name="Ada")
        )
    )

    ref = await chancy.push(chancy.serialize(serialized))
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.SUCCEEDED
    assert j.meta["greeting"] == "hello Ada"


@with_greeting_validator(Trigger())
@pytest.mark.asyncio
async def test_trigger_jobs_are_validated(
    chancy: Chancy, worker: Worker, users_table: str
):
    """
    A trigger's job template is validated when the trigger is registered, and
    the jobs it inserts are validated again when they run.
    """
    await chancy.declare(Queue("default"))
    user_id = uuid.uuid4()

    await Trigger.register_trigger(
        chancy,
        table_name=users_table,
        operations=["INSERT"],
        job_template=greet_job.with_kwargs(
            greeting=Greeting(user_id=user_id, name="Ada")
        ),
    )
    async with chancy.pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            sql.SQL("INSERT INTO {table} DEFAULT VALUES").format(
                table=sql.Identifier(users_table)
            )
        )
        await cursor.execute(
            sql.SQL("SELECT id FROM {jobs} WHERE func = %s").format(
                jobs=sql.Identifier(f"{chancy.prefix}jobs")
            ),
            (importable_name(greet),),
        )
        (job_id,) = await cursor.fetchone()

    j = await chancy.wait_for_job(Reference(job_id), timeout=30)

    assert j.state == j.State.SUCCEEDED
    assert j.kwargs == {
        "greeting": {"user_id": str(user_id), "name": "Ada", "times": 1}
    }
    assert j.meta["greeting"] == "hello Ada"


@with_greeting_validator(Trigger())
@pytest.mark.asyncio
async def test_trigger_rejects_invalid_job_template(
    chancy: Chancy, users_table: str
):
    """
    An invalid job template is rejected when the trigger is registered,
    rather than failing every job the trigger would insert.
    """
    with pytest.raises(JobValidationError, match="times must be at most 3"):
        await Trigger.register_trigger(
            chancy,
            table_name=users_table,
            operations=["INSERT"],
            job_template=greet_job.with_kwargs(
                greeting=Greeting(user_id=uuid.uuid4(), name="Ada", times=9)
            ),
        )


@with_greeting_validator(Cron())
@pytest.mark.asyncio
async def test_cron_rejects_invalid_job(chancy: Chancy):
    """
    An invalid job is rejected when it is scheduled, rather than failing
    each time the schedule runs.
    """
    with pytest.raises(JobValidationError, match="times must be at most 3"):
        await Cron.schedule(
            chancy,
            "*/5 * * * *",
            greet_job.with_kwargs(
                greeting=Greeting(user_id=uuid.uuid4(), name="Ada", times=9)
            ).with_unique_key("invalid_greeting"),
        )

    assert await count_rows(chancy, "cron") == 0


@with_greeting_validator(ImmediateLeadership(), WorkflowPlugin())
@pytest.mark.asyncio
async def test_workflow_rejects_invalid_step(chancy: Chancy):
    """
    A workflow with an invalid step is rejected when it is pushed, before any
    of its steps is stored.
    """
    workflow = (
        Workflow("greetings")
        .add(
            "valid",
            greet_job.with_kwargs(
                greeting=Greeting(user_id=uuid.uuid4(), name="Ada")
            ),
        )
        .add(
            "invalid",
            greet_job.with_kwargs(
                greeting=Greeting(user_id=uuid.uuid4(), name="Ada", times=9)
            ),
        )
    )

    with pytest.raises(JobValidationError, match="times must be at most 3"):
        await WorkflowPlugin.push(chancy, workflow)

    assert await count_rows(chancy, "workflows") == 0


async def wait_for_final_states(
    chancy: Chancy, unique_keys: list[str]
) -> dict[str, str]:
    async with chancy.pool.connection() as conn, conn.cursor() as cursor:
        for _ in range(60):
            await cursor.execute(
                sql.SQL(
                    """
                    SELECT unique_key, state FROM {jobs}
                    WHERE unique_key = ANY(%s)
                    AND state IN ('succeeded', 'failed')
                    """
                ).format(jobs=sql.Identifier(f"{chancy.prefix}jobs")),
                (unique_keys,),
            )
            states = dict(await cursor.fetchall())
            if len(states) == len(unique_keys):
                return states
            await asyncio.sleep(0.5)
    raise TimeoutError(f"Jobs {unique_keys} did not finish.")


drifted_jobs = pytest.mark.parametrize(
    "drifted",
    [
        pytest.param(
            greet_job.with_kwargs(greeting={"user_id": "nope", "name": "Ada"}),
            id="model-changed",
        ),
        pytest.param(
            Job(
                func="broken_job_module.greet",
                kwargs={
                    "greeting": {"user_id": str(uuid.uuid4()), "name": "A"}
                },
            ),
            id="module-broken",
        ),
    ],
)


@pytest.fixture
def broken_job_module(tmp_path, monkeypatch):
    """
    A job module that fails to import, as after a refactor removed a name it
    imports.
    """
    (tmp_path / "broken_job_module.py").write_text(
        "from json import does_not_exist\n\n\ndef greet(greeting): ...\n"
    )
    monkeypatch.syspath_prepend(tmp_path)
    monkeypatch.delitem(sys.modules, "broken_job_module", raising=False)


@drifted_jobs
@with_greeting_validator(Cron(poll_interval=1))
@pytest.mark.asyncio
async def test_drifted_cron_job_does_not_block_other_schedules(
    chancy: Chancy, worker: Worker, drifted: Job, broken_job_module
):
    """
    A scheduled job that no longer passes validation, because its model
    changed or its module no longer imports, is still pushed as stored and
    fails when it runs, like any failing job, without keeping the other due
    schedules from being pushed.
    """
    await chancy.declare(Queue("default"))
    for key in ("good", "drifted"):
        await Cron.schedule(
            chancy,
            "* * * * *",
            greet_job.with_unique_key(key).with_kwargs(
                greeting=Greeting(user_id=uuid.uuid4(), name="Ada")
            ),
        )
    async with chancy.pool.connection() as conn, conn.cursor() as cursor:
        # Stored as it was before it stopped passing validation.
        await cursor.execute(
            sql.SQL(
                """
                UPDATE {cron} SET
                    next_run = NOW(),
                    job = CASE unique_key WHEN 'drifted' THEN %s::jsonb ELSE job END
                """
            ).format(cron=sql.Identifier(f"{chancy.prefix}cron")),
            (Json(drifted.with_unique_key("drifted").pack()),),
        )

    states = await wait_for_final_states(chancy, ["good", "drifted"])

    assert states == {"good": "succeeded", "drifted": "failed"}


@drifted_jobs
@with_greeting_validator(ImmediateLeadership(), WorkflowPlugin())
@pytest.mark.asyncio
async def test_drifted_workflow_step_does_not_block_other_workflows(
    chancy: Chancy, worker: Worker, drifted: Job, broken_job_module
):
    """
    A workflow step that no longer passes validation, because its model
    changed or its module no longer imports, is still pushed as stored and
    fails when it runs, failing its workflow like any failing step, without
    keeping the other workflows from advancing.
    """
    await chancy.declare(Queue("default"))
    good = Workflow("good").add(
        "greet",
        greet_job.with_kwargs(
            greeting=Greeting(user_id=uuid.uuid4(), name="Ada")
        ),
    )
    drifted_workflow = Workflow("drifted").add("greet", drifted)
    await WorkflowPlugin.push(chancy, good)
    async with (
        chancy.pool.connection() as conn,
        conn.transaction(),
        conn.cursor(row_factory=dict_row) as cursor,
    ):
        # Stored as it was before it stopped passing validation.
        await WorkflowPlugin._persist_workflow(cursor, chancy, drifted_workflow)

    good = await WorkflowPlugin.wait_for_workflow(chancy, good.id, timeout=30)
    drifted_workflow = await WorkflowPlugin.wait_for_workflow(
        chancy, drifted_workflow.id, timeout=30
    )

    assert good.state == Workflow.State.COMPLETED
    assert drifted_workflow.state == Workflow.State.FAILED
    assert drifted_workflow.steps["greet"].state == QueuedJob.State.FAILED
