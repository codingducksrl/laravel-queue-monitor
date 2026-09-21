# Laravel Queue Monitor

Sends queue signals — how many jobs are waiting, in progress, completed and failed, per job
class — to a monitoring system. The backend is pluggable; AWS CloudWatch via the Embedded
Metric Format (EMF) ships with it.

## Requirements

- PHP 8.3+
- Laravel 13

## Installation

```bash
composer require codingducksrl/laravel-queue-monitor
```

```bash
php artisan vendor:publish --tag=queue-monitor-config
```

Pick a sink, tell the sampler which queues to read, and schedule it:

```php
// config/queue-monitor.php
'sink' => 'emf',
'queues' => ['redis' => ['default', 'high']],
```

```php
// routes/console.php
Schedule::command('queue-monitor:sample')->everyMinute()->withoutOverlapping()->onOneServer();
```

`onOneServer()` matters. Depth is a gauge, so if every app server samples it you get one
datapoint per server per minute and `Sum` reports N times the truth.

## How it works

Depth is read from the queue driver at sample time, so it cannot drift or go stale.
Throughput has to be counted as it happens, because a finished job leaves no trace in the
queue — so `JobQueued`, `JobProcessed` and `JobFailed` each do a single atomic
`Cache::increment()` into a shared store. Nothing is buffered in PHP memory: a worker killed
mid-job loses nothing but the job it was running.

`queue-monitor:sample` then reads the gauges, drains the counters, and hands one batch to the
sink. It reads a counter, publishes it, and only then subtracts exactly what it read, so an
increment landing mid-drain survives and a crash re-publishes rather than loses.

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
`JobClass` dimension.

`FailedJobsTotal` is a backlog gauge and drops when someone runs `queue:retry`; alarm on
`JobsFailed` instead. Drivers that cannot answer simply do not publish — SQS reports no
per-class breakdown, and a failed job provider that cannot count (DynamoDB) publishes no
`FailedJobsTotal`. A missing metric shows as `INSUFFICIENT_DATA`, which is honest; a zero
would keep the alarm green forever.

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

### Writing through a log channel

Set `emf.channel` to route through Laravel instead of stdout. The channel **must** emit the
raw message: the default formatter's `[2026-01-01 00:00:00] production.INFO:` prefix makes the
line unparseable, and the metrics disappear with no error anywhere.

```php
// config/logging.php
'emf' => [
    'driver' => 'monolog',
    'handler' => Monolog\Handler\StreamHandler::class,
    'handler_with' => ['stream' => 'php://stdout'],
    'formatter' => Monolog\Formatter\LineFormatter::class,
    'formatter_with' => ['format' => "%message%\n", 'ignoreEmptyContextAndExtra' => true],
],
```

Omitting `formatter` is not neutral — Laravel installs the decorating formatter in its place.

### Cost

CloudWatch bills per custom metric, and a custom metric is one metric name paired with one
dimension tuple. Seven metrics on one queue is seven; adding a per-class breakdown over 50
classes is another 200. `max_job_classes` bounds it: the busiest classes keep their own
dimension and the rest are folded into `__other__`, so the per-class values still add up to
the queue total.

## Counter store

Counters go through Laravel's cache, so any store that increments atomically across processes
works — redis, memcached, database, dynamodb. Point `counters.store` at one of them.

`array` and `file` are rejected at boot: `array` is a per-process buffer, and `file` is not
atomic under concurrency. Both would silently lose counts.

If the store is Redis, keep it off a `maxmemory-policy` of `allkeys-lru`. The counter hash
carries no TTL, so `volatile-*` and `noeviction` leave it alone, but `allkeys-*` can evict it
and take a window of counts with it.

## Another monitoring system

Implement `MetricSink` and register it:

```php
use CodingDuck\QueueMonitor\MetricSinkManager;

$this->app->make(MetricSinkManager::class)->extend('datadog', function ($app) {
    return new DatadogSink($app->make('datadog.client'));
});
```

The closure is rebound to the manager, so it cannot be a static closure or a first-class
callable. Then set `'sink' => 'datadog'`.

A `Metric` carries a name, a numeric value, a unit and a flat dimension map — no CloudWatch
concepts leak into it.

## Commands

```bash
php artisan queue-monitor:status                       # effective configuration
php artisan queue-monitor:sample                       # sample, publish, drain
php artisan queue-monitor:sample --dry-run             # print without publishing or draining
php artisan queue-monitor:sample redis:high,database   # explicit connection:queue pairs
```

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `true` | Master switch. When off, no listeners are registered at all. |
| `connections` | `[]` | Connections to monitor. Empty means every connection. |
| `queues` | `[]` | Connection to queue map the sampler reads. Empty falls back to the default. |
| `sink` | `null` | `emf`, `null`, or a driver you registered. |
| `emf.namespace` | `Laravel/Queue` | CloudWatch namespace. |
| `emf.channel` | `null` | Log channel to write through. Null writes to stdout. |
| `emf.entity` | `[]` | `Service` and `Environment` root fields. |
| `counters.store` | `null` | Cache store for counters. Null uses the default. |
| `counters.prefix` | `queue-monitor` | Cache key prefix. |
| `max_job_classes` | `25` | Classes keeping their own dimension before folding. |

## Development

```bash
composer test          # analyse + lint:check + type coverage + unit
composer analyse       # PHPStan / Larastan, level 10
composer lint          # Pint, fix in place
composer test:unit     # Pest
```

### Sail

The repository ships a Sail environment so the whole toolchain can run in Docker.
`laravel.test` is an idle container (`sleep infinity`) rather than a web server, because this
is a package and not an application — you exec into it rather than browsing it.

```bash
./vendor/bin/sail up -d
sail composer test
sail bin testbench queue-monitor:sample --dry-run
```

Postgres and Redis come up alongside it, reachable from the container as `pgsql` and `redis`
and from the host on ports `5433` and `6380` (`FORWARD_DB_PORT`, `FORWARD_REDIS_PORT`). The
package's own tests use Testbench's SQLite connection, so `composer test` needs neither.

## License

MIT. See [LICENSE.md](LICENSE.md).
