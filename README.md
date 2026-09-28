# Laravel Queue Monitor

Sends queue signals — how many jobs are waiting, in progress, completed and failed, with an
optional per job class breakdown of the throughput — to a monitoring system. The backend is
pluggable; AWS CloudWatch via the Embedded Metric Format (EMF) ships with it.

## Requirements

- PHP 8.3+
- Laravel 13.26+
- A Redis or DynamoDB cache store for the counters (see [Counter store](#counter-store))

## Installation

```bash
composer require codingducksrl/laravel-queue-monitor
```

```bash
php artisan vendor:publish --tag=queue-monitor-config
```

Turn monitoring on, pick a counter store, and list the queues to monitor:

```dotenv
QUEUE_MONITOR_ENABLED=true
QUEUE_MONITOR_CACHE_STORE=metrics
```

`metrics` is a Redis cache store with a connection of its own, defined as shown under
[Counter store](#counter-store). Laravel's stock `redis` store is refused: its
`lock_connection` is `default`, the connection the Redis queue uses.

```php
// config/queue-monitor.php
'queues' => ['redis' => ['default', 'high']],
```

Then schedule the sampler:

```php
// routes/console.php
Schedule::command('queue-monitor:sample')
    ->everyMinute()
    ->onOneServer()
    ->runInBackground()
    ->appendOutputTo('/proc/1/fd/1');
```

`appendOutputTo()` is not optional. The scheduler sends a command's output to `/dev/null`
unless told otherwise, so the metrics would never leave the box; the sampler refuses to write
into `/dev/null` and fails instead of draining the counters. Point it at whatever your log
shipper collects. `/proc/1/fd/1` is the container's stdout, but only PID 1's user can open it:
for any other user the shell fails the redirect before the sampler starts, and nothing is
reported. On a VM, use a file the CloudWatch agent tails.

Depth is a gauge, so if every app server samples it `Sum` reports N times the truth; hence
`onOneServer()`, which needs the default cache store to be shared between servers. The sampler
also takes a lock per queue in the counter store, so two runs never drain the same counters.
Queues are sampled one after another, so list slow ones last.

Check the setup before traffic arrives. This builds the counter store, the sink and every
monitored queue connection without connecting to them, and exits non-zero when one cannot be
built, so it works as a deploy step. While monitoring is disabled it only warns.

```bash
php artisan queue-monitor:status
```

## How it works

Depth is read from the queue driver at sample time, so it cannot drift or go stale.
Throughput is counted as it happens: `JobQueued`, `JobProcessed` and `JobFailed` each
increment a counter in a shared cache store. Nothing is buffered in PHP memory, so a worker
killed mid-job loses nothing but the job it was running.

On the hot path that is one round trip per event (a single `INCRBY` on Redis) once the
connection is open, and two for a few events of each class after every sample
([details](docs/counting.md#cost-per-event)); under PHP-FPM the first event of each request
also opens the connection (connect, `AUTH`, `SELECT`). A bulk dispatch or `Bus::batch()` of N
jobs adds N round trips; see [Cost](#cost) for classes past the cap. Counting never throws
into your code: a failure is reported to your exception handler and counting stands down for a
minute in that process.

`queue-monitor:sample` then reads the gauges, drains the counters, and hands one batch per
queue to the sink. It reads a counter, publishes it, and only then subtracts exactly what it
read, so an increment landing mid-drain survives and a crash re-publishes rather than loses.
A queue that fails is reported and skipped; the others are still published.

## Metrics

Published against `Connection` and `Queue`:

| Metric | Unit | Source |
|---|---|---|
| `JobsPending` | Count | jobs waiting to run |
| `JobsDelayed` | Count | jobs not yet available |
| `JobsInProgress` | Count | jobs reserved by a worker |
| `FailedJobsTotal` | Count | rows in the failed job store |
| `JobsQueued` | Count | dispatched since the last sample |
| `JobsCompleted` | Count | processed since the last sample |
| `JobsFailed` | Count | permanently failed since the last sample |

`JobsQueued`, `JobsCompleted` and `JobsFailed` are also published with a `JobClass`
dimension: the job's display name, which is its class unless it defines `displayName()`. A
display name becomes a permanent CloudWatch dimension, so keep IDs and personal data out of
it. Set `max_job_classes` to `0` to publish the queue totals only.

The throughput totals are published every minute, as `0` when nothing happened, so a
"nothing completed" alarm fires on a stalled queue instead of sitting in `INSUFFICIENT_DATA`.
Per-class values are published only for classes that had activity.

`FailedJobsTotal` is a backlog gauge and drops when someone runs `queue:retry`; alarm on
`JobsFailed` instead. A failed job provider that cannot count (DynamoDB, the null provider,
Laravel Cloud's managed queues) publishes no `FailedJobsTotal` rather than a zero that would
keep an alarm green. The file provider keeps at most its `limit` (100 by default) on the host
that samples, so its gauge is capped and only sees that host's failures.

What counts as what:

- A job that releases itself (`release()`, or `RateLimited` / `WithoutOverlapping` by default)
  is not completed, and one that calls `fail()` is failed. An attempt that throws counts only
  once the job fails for good. Every other job counts as completed, including one deleted
  without running: debounced, skipped by middleware (`Skip`, a cancelled batch, `dontRelease()`)
  or dropped by `deleteWhenMissingModels`.
- A job retried with `queue:retry` completes without a matching `JobsQueued`.
- A dispatch is counted when it is pushed, even inside a transaction that later rolls back. On
  Redis or SQS the job still runs; on a database queue sharing your connection the rollback
  removes it, so dispatch such jobs `afterCommit`.
- With the phpredis client, `Bus::batch()`, `Bus::bulk()` and `Queue::bulk()` on a Redis queue
  count `JobsQueued` before the pipelined push is sent, so a bulk push that fails is still
  counted. The database driver fires no `JobQueued` for them at all.
- SQS workers report the full queue URL, and forwarded queues are reported under their target
  or, by Redis workers, under the name the worker was started with. All of them are mapped
  back to the configured name. List a forwarded queue under the name it is forwarded from: if
  only its target is listed, failed jobs stored by Redis workers under the source name are not
  counted.
- Sync, deferred and background connections fire no `JobQueued`, so they publish no
  `JobsQueued`, and their jobs do not say which queue they were pushed to, so they count
  towards the first queue listed for the connection. A background connection is monitored as
  `sync`, which runs its jobs.
- A failover connection is monitored as its first connection, the one it pushes to and pops
  from. Jobs that land on a later connection during a failover are not counted unless that
  connection is listed too.
- On SQS, a job released with a delay or backing off after an exception counts as
  `JobsInProgress`, and `JobsDelayed` reflects only `DelaySeconds` given at dispatch.

Throughput drained at a sample covers the time since the last one and is stamped with the
sample time. A run that is skipped (lock held, failure, maintenance mode) folds its window into
the next datapoint, so alarm on `Sum` over two or more periods; after a scheduler outage that
one datapoint holds the whole outage. Add `->evenInMaintenanceMode()` to the schedule if you
want gauges during maintenance.

## CloudWatch EMF

Nothing is sent to AWS from the package and no SDK is involved. Each sample writes JSON lines
to stdout, one for a queue's totals and one per active job class, and the platform's log
driver — `awslogs` on ECS, fluent-bit on EKS, the Lambda runtime — carries them to CloudWatch
Logs, which extracts the metrics. A document, trimmed to one metric:

```json
{"Connection":"redis","Queue":"default","JobsPending":42,"_aws":{"Timestamp":1789981200000,"CloudWatchMetrics":[{"Namespace":"Laravel/Queue","Dimensions":[["Connection","Queue"]],"Metrics":[{"Name":"JobsPending","Unit":"Count"}]}]}}
```

Dimension values are transliterated to printable ASCII, which is all CloudWatch accepts. A
value that had to change gets `#` and an 8-digit hash of the original appended, so two names
never share a series: `Café` is published as `Cafe#9aef6e66`. Run
`queue-monitor:sample --dry-run` to see the exact values before building alarms or dashboards
on them.

### Cost

CloudWatch bills per custom metric, prorated by the hour, and a custom metric is one metric
name paired with one dimension tuple. The queue-level metrics are up to seven per queue; the
per-class breakdown adds three per class, plus three for `__other__`, so the default cap of 25
adds up to 78 per queue. `max_job_classes` bounds it: the first classes seen on a queue keep
their own dimension and every other class is folded into `__other__`, so the per-class values
still add up to the queue total and the set of published series stays fixed from one hour to
the next. `0` turns the breakdown off. To pick the classes again, change `counters.prefix`,
which also drops the counts not yet published.

Up to four times `max_job_classes` names per queue (100 by default) are counted at the usual
cost and folded when sampled. A name seen after that is counted straight into `__other__`, so
runaway names (a `displayName()` with an ID in it) cannot grow what the sampler reads. They
are not free, though: the first event of each such name costs about five round trips, later
events of a repeating one cost two, and each leaves a key that lives for two minutes.

## Counter store

Point `counters.store` at a Redis store. DynamoDB also works, but costs an HTTPS round trip per
job event, holds one item per class and metric (which caps a single class, or a whole queue
with `max_job_classes` at `0`, at DynamoDB's per-item write rate), and reads eventually
consistently: a class registered by two processes at once can be missed, and a released
sampler lock can linger until it expires. On any store, a class whose registration is missed
that way, or because the registry lock was busy, keeps counting and is published once one of
its next events registers it again, at most 100 events later.

Every other store is refused, and so is a Redis store whose connection or `lock_connection` is
not defined or is also used by a Redis queue: bulk dispatch pushes inside a `MULTI` on that
connection, where a counter command is only queued. A refused store counts nothing and is
reported like any other store failure; `queue-monitor:status` fails. The check is skipped
while running tests (`APP_ENV=testing`).

Give the counters a Redis connection of their own, with tight timeouts, so a slow Redis cannot
hold up your requests. Without `timeout` and `read_timeout`, phpredis falls back to PHP's
`default_socket_timeout` (60 s by default). `max_retries` lets a counter survive a connection
Redis dropped (a failover, an idle timeout) instead of pausing counting for a minute. Leave
`command_retries` unset: it re-sends an increment whose reply timed out.

```php
// config/database.php, under 'redis'
'metrics' => [
    'host' => env('REDIS_METRICS_HOST', '127.0.0.1'),
    'username' => env('REDIS_METRICS_USERNAME'),
    'password' => env('REDIS_METRICS_PASSWORD'),
    'port' => env('REDIS_METRICS_PORT', '6379'),
    'database' => env('REDIS_METRICS_DB', '3'),
    'timeout' => 0.2,
    'read_timeout' => 0.2,       // phpredis
    'read_write_timeout' => 0.2, // predis
    'max_retries' => 1,          // phpredis
],

// config/cache.php, under 'stores'
'metrics' => ['driver' => 'redis', 'connection' => 'metrics', 'lock_connection' => 'metrics'],
```

Under PHP-FPM every request starts afresh, so the one-minute stand-down after a failure cannot
carry from one request to the next: the timeouts are what bound the cost there, and the
failure is reported once per affected request. Throttle it with `$exceptions->throttle()` in
`bootstrap/app.php` if that is too loud.

Keep the Redis `maxmemory-policy` off `allkeys-*`. Counters carry no TTL, so `volatile-*` and
`noeviction` leave them alone, but `allkeys-*` can evict them and take a window of counts with
it. Flushing the store loses the current window only, and counting resumes on its own, but
`FLUSHDB` clears the whole database: never flush a store that shares one with your queues.

## Sampling cost

Each sample costs, per monitored queue, three depth reads from the driver, one or two
failed-job counts (two when workers record the queue under another name: an SQS URL, `sync`,
a forward target), one cache read for the job classes and one per 100 counters, one decrement
per non-zero counter, and three round trips for the sampler lock.

- **Database queue**: three `COUNT` queries on the `jobs` table. They do not block workers,
  but on a large backlog they are real work.
- **SQS**: three `GetQueueAttributes` calls per queue.
- **Failed jobs**: a `COUNT` on `failed_jobs` by connection and queue. The current Laravel
  migration indexes `connection`, `queue` and `failed_at` for it. Tables from older skeletons
  have `TEXT` columns and no index, so each sample scans the table; change `connection` and
  `queue` to `string` and add `$table->index(['connection', 'queue'])`, or prune the table.

## Another monitoring system

Implement `MetricSink` and register it, typically in a service provider's `boot()`:

```php
use CodingDuck\QueueMonitor\MetricSinkManager;

$this->app->make(MetricSinkManager::class)->extend('datadog', function ($app) {
    return new DatadogSink($app->make('datadog.client'));
});
```

Then set `QUEUE_MONITOR_SINK=datadog`. Each `write()` call receives one queue's batch. A
`Metric` carries a name, an integer count and a flat dimension map (`Connection`, `Queue`, and
`JobClass` on the per-class values), not cleaned for any backend. Throw when delivery fails:
the counters are drained as soon as `write()` returns, so a sink that swallows an error loses
that window. `--dry-run` prints dimension values cleaned the way the EMF sink cleans them,
whatever the sink.

## Commands

```bash
php artisan queue-monitor:status                              # configuration and readiness check
php artisan queue-monitor:sample                              # what the schedule runs: publish and drain
php artisan queue-monitor:sample --dry-run                    # print without publishing or draining
php artisan queue-monitor:sample redis:high,database:default  # explicit connection:queue pairs
```

Run by hand, `queue-monitor:sample` publishes to your terminal and drains the counters, so
that window never reaches CloudWatch; use `--dry-run` to look. A bare entry in the argument
(`high`) is a queue on the default connection, and a failover connection stands for its first
connection, as in `queues`. Counters exist only for the queues in `queues`, so an explicit pair
outside it publishes depth but no throughput.

## Configuration

| Key | Env | Default | Meaning |
|---|---|---|---|
| `enabled` | `QUEUE_MONITOR_ENABLED` | `false` | Master switch. When off, no listeners are registered at all. |
| `queues` | | `[]` | Connection to queue map that is counted and sampled. A connection listed without queues monitors its default queue; an empty map, the default connection's default queue. |
| `sink` | `QUEUE_MONITOR_SINK` | `emf` | `emf` or a driver you registered. |
| `emf.namespace` | `QUEUE_MONITOR_NAMESPACE` | `Laravel/Queue` | CloudWatch namespace: up to 255 of `A-Z a-z 0-9 . - _ / # :` and space, not starting with `:`, a space or `AWS/`. |
| `emf.entity` | `QUEUE_MONITOR_SERVICE`, `QUEUE_MONITOR_ENVIRONMENT` | `[]` | `Service` and `Environment` fields added to every document, not as dimensions. |
| `counters.store` | `QUEUE_MONITOR_CACHE_STORE` | `null` | Cache store for counters. Null uses the default. |
| `counters.prefix` | `QUEUE_MONITOR_CACHE_PREFIX` | `queue-monitor` | Cache key prefix. |
| `max_job_classes` | `QUEUE_MONITOR_MAX_JOB_CLASSES` | `25` | Classes keeping their own `JobClass` dimension before folding; `0` publishes queue totals only. |

## Contributing

How the package works inside, how to run its checks and what the tests hold it to:
[docs/](docs/README.md).

## License

MIT. See [LICENSE.md](LICENSE.md).
