Metrics
=======

Metrics describe observations, not an authoritative count of jobs in a queue.
They are buffered in memory, flushed every 60 seconds by default, and read from
PostgreSQL. An abrupt worker exit can lose observations that have not been
flushed. Graceful shutdown flushes metrics after final job updates.

Meaning and units
-----------------

* Counters sum observed increments within the requested window.
* Gauges select the most recently sampled value, including across workers.
* Histograms summarize count, sum, minimum and maximum; averages are weighted by
  observation count. These summaries do not support percentiles.
* Job state and throughput counters count accepted, committed job update events,
  including retries. They do not measure queue occupancy, unique jobs, or every
  state transition: claiming, manual cancellation and recovery use other paths.
* Job and queue execution times are in seconds and measure the final attempt for
  updates containing both start and completion timestamps.
* Workflow counters describe committed scheduler transitions. Workflow duration
  is the elapsed time from creation to the scheduler's terminal update, including
  dependency waits and scheduler delay. Producer submissions are not counted.
* Table sizes are gauges in bytes, sampled by the leader only.

Custom observations can specify ``unit`` on
:meth:`~chancy.worker.Worker.increment_counter`,
:meth:`~chancy.worker.Worker.record_gauge` and
:meth:`~chancy.worker.Worker.record_histogram_value`. A key's type and unit must
remain consistent. Keys may contain up to 1,024 UTF-8 bytes; values must be finite
numbers. Invalid observations are dropped with a rate-limited warning.

Ranges, gaps and freshness
--------------------------

``GET /api/v1/metrics/{prefix}`` accepts a ``resolution`` and a separate ``range``
in seconds. For example, ``?resolution=5min&range=86400`` requests 24 hours at
five-minute resolution. ``worker_id`` optionally limits the observations to one
worker. A prefix matches the exact key and descendants separated by ``:``.

The response contains ``start``, ``end``, ``resolution``, ``generated_at`` and a
``series`` mapping. Each series contains ``type``, ``unit``, ``aggregation``,
``sampled_at``, ``summary`` and chronological ``data`` points. Each point contains
its bucket ``timestamp``, last observation's ``sampled_at``, and ``value``.
``summary`` uses the same reducer as the metric type over the returned points.
An empty series has a null summary and null sample time.

Windows are UTC-aligned and half-open: ``[start, end)``. The default includes the
current, potentially incomplete bucket. Alternatively, pass timezone-aware ISO
``start`` and ``end`` timestamps aligned to the chosen resolution. ``limit`` is
also supported as a number of time buckets, not a number of populated samples;
it cannot be combined with ``start`` or ``range``. Invalid windows return HTTP 422.
Requests are limited to 1,000 buckets and 100 matching series. Read results may
be cached for ten seconds.

Missing buckets mean no observation, not zero. Histograms and totals cover only
observed samples; sample timestamps show how recently a metric was observed.
The dashboard leaves gaps in charts and displays an unavailable value where no
summary exists. A success rate requires observations for both outcomes; an
absent failure series is not evidence of zero failures.

Retention and resource use
--------------------------

Default retention is elapsed time, rather than a count of populated samples:

.. list-table::
   :header-rows: 1

   * - Resolution
     - Retention
   * - 1 minute
     - 1 hour
   * - 5 minutes
     - 24 hours
   * - 1 hour
     - 7 days
   * - 1 day
     - 90 days

Expired samples are excluded from reads even before the Pruner removes them.
Storage groups at most 12 buckets into a chunk; pruning removes fully expired
chunks, so a boundary chunk can temporarily contain older samples. Each pass
removes at most 10,000 chunks. Unreferenced metric definitions are also pruned.

Workers retain current chunks and pending writes instead of reading all workers'
history. Only changed chunks are written; retries replace the same worker-session
snapshot without counting it again. Reusing a worker ID preserves previous
sessions. More frequent worker restarts create additional chunks.

The in-memory buffer defaults to 10,000 bucket snapshots. If persistence is
unavailable, dirty observations are retried with backoff, within retention and
this memory bound. New buckets exceeding the bound are dropped with a warning.
The API bounds its local read cache to 32 windows and 50,000 points.

Upgrading
---------

Metrics migration 2 clears existing metric history and replaces its storage
layout. Previous timestamps, gauges and sparse retention cannot be translated
reliably into the new contract. Jobs, queues and workflows are unaffected.
Stop workers and API processes before migrating, then restart them together;
the old collector and API are incompatible with the new schema. Downgrading
also clears metric history.

Reference
---------

.. automodule:: chancy.plugins.metrics
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: chancy.plugins.metrics.metrics.Metric
   :members:

.. autoclass:: chancy.plugins.metrics.metrics.MetricPoint
   :members:
