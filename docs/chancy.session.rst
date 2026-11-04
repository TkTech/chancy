chancy.session module
=====================

Plugin lifecycle
----------------

Each worker creates a session for each plugin and calls ``setup(session)``
before starting background work. Register periodic callbacks with
``session.periodic(...)`` and one-shot or long-running services with
``session.service(...)``. Callbacks receive an ``OperationContext`` containing
the worker, application and logger. An operation handle's ``wake_up()`` requests
an early run without postponing the scheduled deadline.

Worker supervision runs independently of ``wait_for_shutdown()``. An unhandled
operation error, exhausted database retries, or an unexpected maintenance-task
exit initiates worker shutdown. Services may return successfully; periodic
operations run until cancelled. Session closure and worker shutdown request
cancellation through the same task manager so asynchronous cleanup is not
interrupted repeatedly.

``OperationContext.database()`` retries database failures and explicit
``chancy.retry.RetryTransaction`` conflicts in fresh transactions. Callbacks
must be safe to repeat, including after an uncertain commit. Other callback
errors are terminal; arbitrary external side effects are not automatically
replayed. Deadlocks, serialization failures and explicit transaction conflicts
do not invalidate the worker's leadership flag.

Subscriptions registered through ``session.subscribe()`` are observers. A
handler exception is logged and delivery continues to other handlers;
cancellation propagates. Resources registered with ``session.callback()`` are
released in reverse order after background tasks stop and final persistence
hooks succeed. Cleanup callbacks must be synchronous.

Migrating plugins
-----------------

Replace ``run(worker, chancy)`` and ``on_worker_started(worker=...)`` with
``setup(session)``. Replace manual loops and ``Plugin.sleep()`` with periodic
operations, and replace ``Plugin.wake_up()``/``wakeup_signal`` with the returned
operation's ``wake_up()``. Use ``authority=Authority.LEADER`` instead of
``wait_for_leader()`` and put protected database effects inside
``ctx.database(...)``.

The cleanup hook is now ``cleanup(chancy, cursor)``. Use the supplied cursor;
do not open a separate transaction or commit it. Pruner schedules each cleanup
as a separate leader-authority operation, bounded by the application's pool
capacity. Cleanup hooks must not depend on another plugin's cleanup order.

The former lifecycle method names remain reserved and are rejected during
worker startup, including inherited definitions. ``cleanup_ex`` was not a
public hook and is not reserved. Plugins using only unchanged job hooks can
continue to use them.

Leadership protocol
-------------------

Leadership is owned by Worker and requires no plugin. Every eligible worker
attempts election immediately, then renews or retries election every
``leadership_poll_interval`` seconds (default 60). The lease lasts
``leadership_timeout`` seconds (default 120). Both must be finite and positive;
the timeout must exceed the poll interval, and twice it is recommended.
Set ``leadership_eligible=False`` to exclude a worker from election. Its
``Authority.LEADER`` operations remain waiting; worker-authority operations
continue normally. The standalone dashboard worker is ineligible automatically.

``worker.is_leader`` is a read-only boolean observation. Use
``await worker.wait_for_leadership()`` to await acquisition. Neither grants
permission for future database writes: leader-authority transactions verify
row ``id = 1`` in ``{prefix}leader``. Its ``worker_id`` must match the worker and
``expires_at`` must be later than the database's current time. A key-share row
lock prevents takeover until the transaction ends, while allowing renewal. This
protects database effects within the transaction, not calls to external services.

Transient database failures retry with the worker's backoff settings. Connection
failures clear local leadership; transaction conflicts, statement timeouts, and
lock timeouts do not. Local leadership also expires independently of renewal,
including when renewal is blocked on a connection or a row lock. Permanent errors or exhausted retries initiate the
same supervised shutdown as any other core maintenance failure.

Election checks still broadcast ``leadership.gained``, ``leadership.renewed``,
and ``leadership.lost`` when applicable. Local expiry and connection loss can
occur without a notification; events are observations, not write authorization.

Stopping clears local leadership immediately. The database lease remains until
expiry, so takeover can take up to the remaining lease lifetime plus a polling
interval (and longer during database outages or a protected transaction).
Use unique worker IDs; reusing a live worker's ID defeats lease ownership.

Migration: remove ``Leadership(...)`` from plugin lists and move its
``poll_interval`` and ``timeout`` arguments to the Worker options above.
Remove ``chancy.leadership`` from plugin dependency lists. Custom election
plugins and ``ImmediateLeadership`` are no longer supported. Tests should use
real leases. Replace ``worker.is_leader.is_set()`` with ``worker.is_leader`` and
``worker.is_leader.wait()`` with ``worker.wait_for_leadership()``. The existing
core lease table is reused; this change needs no schema migration.

Shutdown and failure reporting
------------------------------

The worker first stops fetching jobs and drains executors for
``shutdown_timeout`` seconds, keeping update processing available during the
drain. It then closes task admission, requests cancellation once per task, and
waits up to ``teardown_timeout`` seconds (default 30) for background cleanup.
Final job updates and plugin hooks run after those tasks stop.

If task teardown times out, unfinished tasks remain tracked and their plugin
resources stay open. Keep the application open and retry ``stop()`` after the
tasks finish; Python cannot force a coroutine to obey cancellation. The task
deadline does not bound final database persistence. Failed persistence can be
retried without repeating completed task teardown or successful plugin hooks.
Terminal session cleanup errors are retained without rerunning finalization.

``stop()`` and ``wait_for_shutdown()`` report background and cleanup failures
after teardown. A single failure is raised directly; multiple failures are
reported in an exception group. Concurrent and later waiters observe the
retained outcome. Cancelling a waiter does not cancel the shared shutdown task.
The task manager logs terminal background failures once; callers can handle
the propagated exceptions without logging them again.

``TaskManager.cancel()`` accepts a task handle, not a name. Task names are
labels and need not be unique. Iteration and ``len()`` describe live tasks;
``errors`` retains failures independently. ``cancel_all()`` permanently closes
admission and rejects later coroutines without leaving them unawaited.

.. autoexception:: chancy.retry.RetryTransaction

.. automodule:: chancy.session
   :members:
   :undoc-members:
   :show-inheritance:
