# Laravel Queue Monitor

Sends queue signals — how many jobs are waiting, in progress, completed and failed, per job
class — to a monitoring system. The backend is pluggable; AWS CloudWatch via the Embedded
Metric Format (EMF) ships with it.

## Requirements

- PHP 8.3+
- Laravel 13.26+
- A Redis, Memcached or DynamoDB cache store for the counters (see [Counter store](#counter-store))

## Installation

```bash
composer require codingducksrl/laravel-queue-monitor
```

```bash
php artisan vendor:publish --tag=queue-monitor-config
```

Monitoring is off until you turn it on. Turn it on, pick a sink and a counter store, and list
the queues to monitor:

```dotenv
QUEUE_MONITOR_ENABLED=true
QUEUE_MONITOR_SINK=emf
QUEUE_MONITOR_CACHE_STORE=metrics
```

`metrics` is a Redis cache store with a connection of its own, defined as shown under
[Counter store](#counter-store). Laravel's stock `redis` store cannot be used as it is: it has
no timeouts, and its `lock_connection` is `default`, the connection the Redis queue uses.

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

`appendOutputTo()` is not optional with the EMF sink's default stdout output. The scheduler
sends a command's output to `/dev/null` unless told otherwise, so the metrics would never
leave the box. The sampler refuses to write into `/dev/null` and fails instead of draining the
counters. Point it at whatever your log shipper collects: `/proc/1/fd/1` is the container's
stdout when the scheduler runs as PID 1 (or as the same user); on a VM, a file the CloudWatch
agent tails.

`onOneServer()` matters too. Depth is a gauge, so if every app server samples it you get one
datapoint per server per minute and `Sum` reports N times the truth. It needs the default cache
store to be shared between servers, as all of Laravel's `onOneServer()` does. The sampler also
takes a lock per queue in the counter store, so two runs never drain the same counters. Queues
are sampled one after another, so a slow one delays those listed after it; list slow queues
last.

Check the setup before traffic arrives; this exits non-zero when the counter store or the
sink cannot be built, so it works as a deploy step. It checks the configuration without
connecting to the store:

```bash
php artisan queue-monitor:status
```

## How it works

Depth is read from the queue driver at sample time, so it cannot drift or go stale.
Throughput has to be counted as it happens, because a finished job leaves no trace in the
queue — so `JobQueued`, `JobProcessed` and `JobFailed` each increment a counter in a shared
cache store. Nothing is buffered in PHP memory: a worker killed mid-job loses nothing but the
job it was running.

On the hot path that is one round trip per event (a single `INCRBY` on Redis) once the
connection is open, and two for a class past the registry cap (see [Cost](#cost)); under
PHP-FPM the first event of each request also opens it (connect, `AUTH`, `SELECT`). A database
counter store is refused on the default connection; give it a connection name nothing else
uses, and counting cannot lock inside or abort your transactions. A dispatch is counted when it is
pushed, so one pushed to Redis or SQS inside a transaction that later rolls back still counts,
as the job still runs. It never throws into your code: a failure is reported to your exception
handler, and counting stands down for a minute in that process so an unreachable store costs
one timeout per minute rather than one per job.

`queue-monitor:sample` then reads the gauges, drains the counters, and hands one batch to the
sink. It reads a counter, publishes it, and only then subtracts exactly what it read, so an
increment landing mid-drain survives and a crash re-publishes rather than loses. Each queue is
sampled on its own: one that fails is reported and skipped, and the others are still published.

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

`JobsQueued`, `JobsCompleted`, `JobsFailed` and `JobsInProgress` are also published with a
`JobClass` dimension: the job's display name, which is its class unless it defines
`displayName()`. A display name becomes a permanent CloudWatch dimension, so keep IDs and
personal data out of it.

The three throughput totals are published every minute, as `0` when nothing happened, so a
"nothing completed" alarm fires on a stalled queue instead of sitting in `INSUFFICIENT_DATA`.
Per-class values are published only for classes that had activity.

`FailedJobsTotal` is a backlog gauge and drops when someone runs `queue:retry`; alarm on
`JobsFailed` instead. Drivers that cannot answer simply do not publish: SQS and Beanstalkd
publish no per-class `JobsInProgress` (their throughput still carries `JobClass`), and a failed job provider that cannot count (DynamoDB, the null
provider, Laravel Cloud's managed queues) publishes no `FailedJobsTotal`. A missing metric
shows as `INSUFFICIENT_DATA`, which is honest; a zero would keep the alarm green forever. The
file provider keeps at most its `limit` (100 by default) on the host that samples, so its
gauge is capped and only sees that host's failures.

What counts as what:

- A job that releases itself (`release()`, `RateLimited`, `WithoutOverlapping`) is not
  completed. A job that calls `fail()` is failed, not completed. A debounced job superseded by
  a newer dispatch is deleted without running and still counts as completed.
- A job retried with `queue:retry` completes without a matching `JobsQueued`.
- With the phpredis client, `Bus::batch()`, `Bus::bulk()` and `Queue::bulk()` on a Redis queue
  count `JobsQueued` before the pipelined push is sent, so a bulk push that fails is still
  counted.
- The database driver fires no `JobQueued` for `Bus::batch()`, `Bus::bulk()` and
  `Queue::bulk()`, so those dispatches are missing from `JobsQueued` while their completions
  are counted.
- SQS workers report the full queue URL. A forwarded queue is reported under its target by
  database and beanstalkd workers, and under the name the worker was started with by Redis
  workers. All of them are mapped back to the configured queue name.
- Sync, deferred and background connections fire no `JobQueued`, so they publish no
  `JobsQueued`, and their jobs do not say which queue they were pushed to, so they count
  towards the first queue listed for the connection. A background connection runs its jobs on
  `sync`, so list `sync` to count them.
- A failover connection is monitored, and reported, as its first connection, which is the one
  it pushes to and pops from. Jobs that land on a later connection during a failover are not
  counted unless that connection is listed too.
- On SQS, a job released with a delay or backing off after an exception counts as
  `JobsInProgress`, and `JobsDelayed` reflects only `DelaySeconds` given at dispatch.

Throughput drained at a sample covers the minute before it, and is stamped with the sample
time. A run that is skipped (lock held, failure, maintenance mode) folds its window into the
next datapoint, so alarm on `Sum` over two or more periods. Add `->evenInMaintenanceMode()` to
the schedule if you want gauges during maintenance.

## CloudWatch EMF

Nothing is sent to AWS from the package and no SDK is involved. The sink writes one JSON line
per dimension tuple to stdout, and the platform's log driver — `awslogs` on ECS, fluent-bit on
EKS, the Lambda runtime — carries it to CloudWatch Logs, which extracts the metrics.

```json
{"_aws":{"Timestamp":1789981200000,"CloudWatchMetrics":[{"Namespace":"Laravel/Queue","Dimensions":[["Connection","Queue"]],"Metrics":[{"Name":"JobsPending","Unit":"Count"}]}]},"Connection":"redis","Queue":"default","JobsPending":42}
```

Each document carries exactly one dimension set. Listing both the queue-level and per-class
sets in one document would publish the queue-level gauge once per job class, and its `Sum`
would come out as depth × class count.

Dimension values are transliterated to printable ASCII, which is all CloudWatch accepts, with
control characters removed. A value that had to change gets `#` and an 8-digit hash of the
original appended, so two names never share a series: `Café` is published as
`Cafe#9aef6e66`. Run `queue-monitor:sample --dry-run` to see the exact values before building
alarms or dashboards on them.

### Writing through a log channel

Set `emf.channel` to route through Laravel instead of stdout. The channel **must** emit the
raw message at the `info` level: the default formatter's `[2026-01-01 00:00:00] production.INFO:`
prefix makes the line unparseable, and the metrics disappear with no error anywhere. A channel
that is not defined, cannot be built (or has a stack member that cannot), drops `info`
records (a level above `info`, including one set in `handler_with`), buffers them
(`action_level`, a buffer handler) or hides write failures (`ignore_exceptions`) is rejected
when the sink is built, so `queue-monitor:status` fails.

```php
// config/logging.php
'emf' => [
    'driver' => 'monolog',
    'handler' => Monolog\Handler\StreamHandler::class,
    'handler_with' => ['stream' => '/var/log/app/emf.log'],
    'formatter' => Monolog\Formatter\LineFormatter::class,
    'formatter_with' => ['format' => "%message%\n", 'ignoreEmptyContextAndExtra' => true],
],
```

Omitting `formatter` is not neutral — Laravel installs the decorating formatter in its place.
A stream of `php://stdout` or `php://stderr` goes to `/dev/null` when the sampler is scheduled
without `appendOutputTo()`, and unlike the default emitter a log channel is not checked for
it: the sample succeeds and the window is lost. Write to a file or socket your log shipper
collects.

### Cost

CloudWatch bills per custom metric, prorated by the hour, and a custom metric is one metric
name paired with one dimension tuple. Seven metrics on one queue is seven; a per-class
breakdown over 50 classes adds another 200. `max_job_classes` bounds it: the first classes
seen on a queue keep their own dimension and every other class is folded into `__other__`, so
the per-class values still add up to the queue total and the set of published series stays
fixed from one hour to the next. To pick again, flush the counter store or change
`counters.prefix`.

The registry behind this tracks four times `max_job_classes` per queue (100 by default) so
that folded classes are still counted individually. A class seen after that is counted
straight into `__other__`, and runaway names (a `displayName()` with an ID in it) cannot grow
the registry or the sampler's reads. They are not free, though: the first event of each such
name costs about five round trips (it reads the whole registry and parks its key), later
events of a repeating one cost two, and each parked key lives for an hour (on a database
store, for good). Keep IDs out of display names.

## Counter store

Point `counters.store` at a Redis store. Memcached and DynamoDB also work. Memcached may evict
counters under memory pressure. DynamoDB costs an HTTPS round trip per job event, holds one
item per class and metric (which caps a single class at DynamoDB's per-item write rate), and
reads eventually consistently, so a class registered by two processes at once can be missed.
On any store, a class whose registration is missed that way, or because the registry lock was
busy, is counted but not published until a re-check finds it, at its 2nd, 4th, 8th … 64th
event and every 100th after that.

A database store is refused on the application's default connection, where it would add a
locking transaction per job to your primary database. Give it a connection name nothing else
uses, even one pointing at the same database: it then has its own PDO and never joins your
transactions. On a database store a parked key is never removed, as Laravel deletes an
expired row only when it is read. `array`, `file`, `null`, `failover` and other stores that cannot increment atomically
across processes are refused too. A refused store is reported when first used and counting
stays off; `queue-monitor:status` fails.

Give the counters a Redis connection of their own, with tight timeouts, so a slow Redis
cannot hold up your requests. Neither it nor the store's `lock_connection` may be the
connection your Redis queue uses: bulk dispatch runs inside a pipeline on that connection.

```php
// config/database.php, under 'redis'
'metrics' => [
    'url' => env('REDIS_URL'),
    'host' => env('REDIS_HOST', '127.0.0.1'),
    'username' => env('REDIS_USERNAME'),
    'password' => env('REDIS_PASSWORD'),
    'port' => env('REDIS_PORT', '6379'),
    'database' => env('REDIS_METRICS_DB', '3'),
    'timeout' => 0.2,
    'read_timeout' => 0.2,       // phpredis
    'read_write_timeout' => 0.2, // predis
    'max_retries' => 0,
],

// config/cache.php, under 'stores'
'metrics' => ['driver' => 'redis', 'connection' => 'metrics', 'lock_connection' => 'metrics'],
```

Under PHP-FPM every request starts afresh, so the one-minute stand-down after a failure
cannot carry from one request to the next: the timeouts are what bound the cost there, and
the failure is reported once per affected request. Throttle it with `$exceptions->throttle()`
in `bootstrap/app.php` if that is too loud.

Keep the Redis `maxmemory-policy` off `allkeys-lru`. Counters carry no TTL, so `volatile-*`
and `noeviction` leave them alone, but `allkeys-*` can evict them and take a window of counts
with it. The one exception is the parked key of a class past the registry cap, which expires
after an hour; evicting it early costs nothing. Flushing the store (`cache:clear` or `optimize:clear` on a shared store included)
loses the counts of the current window only; counting resumes on its own.

## Sampling cost

Each sample costs, per monitored queue, three depth reads from the driver, one or two
failed-job counts (two when workers record the queue under another name: an SQS URL, `sync`,
a forward target), one cache read for the job classes and one per 100 counters, then one decrement per
non-zero counter. While jobs are in progress it also fetches and decodes every reserved job to
break `JobsInProgress` down by class (`ZRANGE` of the reserved set on Redis), so that part
scales with reserved jobs times payload size. SQS and Beanstalkd skip it.

- **Database queue**: two `COUNT` queries and one `SELECT` of the reserved rows on the `jobs`
  table. They do not block workers, but on a large backlog they are real work; leave very
  large database queues out of `queues` if that matters.
- **SQS**: three `GetQueueAttributes` calls per queue.
- **Failed jobs**: a `COUNT` on `failed_jobs` by connection and queue. The current Laravel
  migration indexes `connection`, `queue` and `failed_at` for it. Tables from older skeletons
  have `TEXT` columns and no index, so each sample scans the table; change `connection` and
  `queue` to `string` and add `$table->index(['connection', 'queue'])`, or prune the table.

## Another monitoring system

Implement `MetricSink` and register it:

```php
use CodingDuck\QueueMonitor\MetricSinkManager;

$this->app->make(MetricSinkManager::class)->extend('datadog', function ($app) {
    return new DatadogSink($app->make('datadog.client'));
});
```

Then set `'sink' => 'datadog'`.

A `Metric` carries a name, a numeric value, a unit and a flat dimension map — no CloudWatch
concepts leak into it.

## Commands

```bash
php artisan queue-monitor:status                              # configuration and readiness check
php artisan queue-monitor:sample                              # sample, publish, drain
php artisan queue-monitor:sample --dry-run                    # print without publishing or draining
php artisan queue-monitor:sample redis:high,database:default  # explicit connection:queue pairs
```

A bare entry in the argument (`high`) is a queue on the default connection, and a failover
connection stands for its first connection, as in `queues`. Counters exist only for the queues
in `queues`, so an explicit pair outside it publishes depth but no throughput.

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Master switch. When off, no listeners are registered at all. |
| `queues` | `[]` | Connection to queue map that is counted and sampled. Empty means the default connection's default queue. A failover connection stands for its first connection. |
| `sink` | `null` | `emf`, `null`, or a driver you registered. |
| `emf.namespace` | `Laravel/Queue` | CloudWatch namespace: up to 255 of `A-Z a-z 0-9 . - _ / # :` and space, not `AWS/`. |
| `emf.channel` | `null` | Log channel to write through. Null writes to stdout. |
| `emf.entity` | `[]` | `Service` and `Environment` root fields. |
| `counters.store` | `null` | Cache store for counters. Null uses the default. |
| `counters.prefix` | `queue-monitor` | Cache key prefix (`QUEUE_MONITOR_CACHE_PREFIX`). |
| `max_job_classes` | `25` | Classes keeping their own dimension before folding. |

## Development

```bash
composer test          # analyse + lint:check + type coverage + unit
composer analyse       # PHPStan / Larastan, level 10
composer lint          # Pint, fix in place
composer test:unit     # Pest
```

The Redis tests run against a real server and are skipped unless one is reachable at
`REDIS_HOST`/`REDIS_PORT`; CI provides one.

### Sail

The repository ships a Sail environment so the whole toolchain can run in Docker.
`laravel.test` is an idle container (`sleep infinity`) rather than a web server, because this
is a package and not an application — you exec into it rather than browsing it.

The compose file also attaches the container to an external `proxy` network for a local
Traefik setup, so create it once first:

```bash
docker network create proxy
./vendor/bin/sail up -d
sail composer test
sail bin testbench queue-monitor:sample --dry-run
```

Postgres and Redis come up alongside it, reachable from the container as `pgsql` and `redis`
and from the host on ports `5433` and `6380` (`FORWARD_DB_PORT`, `FORWARD_REDIS_PORT`). The
package's own tests use Testbench's SQLite connection, so `composer test` needs neither.

## License

MIT. See [LICENSE.md](LICENSE.md).
