# Development

## Checks

```bash
composer test          # the four below, as CI runs them
composer analyse       # PHPStan with Larastan, level 10, on src and config
composer lint:check    # Pint, without writing
composer test:types    # Pest type coverage, 100 % required
composer test:unit     # Pest, in parallel
composer lint          # Pint, fixing in place
```

Tests run in random order and fail on output, warnings and risky tests (`phpunit.xml.dist`).

The Redis tests in `tests/Feature/RedisCountersTest.php` need a real server and flush its database
15. Locally they are skipped unless the `redis` extension is loaded and `REDIS_HOST` is set; with
`CI` set they fail instead. They run with the client's serializer and compression options too, which
must never reach a counter: `INCRBY` fails on a serialized number. Sail provides the server:

```bash
docker network create proxy
./vendor/bin/sail up -d
./vendor/bin/sail composer test
```

CI runs `composer test` on PHP 8.3, 8.4 and 8.5 with stable dependencies, and on 8.3 with the
lowest, against a Redis 8 service.

## Tests

| Suite | Covers | With |
|---|---|---|
| `tests/ArchTest.php` | Pest's `php` and `security` presets, no `dd`, `ddd`, `env` or `exit`, strict types | |
| `tests/Unit` | `Counters`, `EmfSink` and `QueueMonitor` on their own | `ArrayStore` and `SpyStore`, `php://memory`, a bare config `Repository` |
| `tests/Feature` | the listener, the commands, the provider, the sink manager, the store guard, EMF end to end | a Testbench app with the `database` queue |

`TestCase` enables monitoring for `database:default`, points the failed job provider at the test
database, sets an app key (Laravel Cloud's failed job provider needs an encrypter) and loads
Laravel's migrations for the `jobs`, `job_batches` and `failed_jobs` tables. The Workbench jobs
`SendInvoice`, `SyncContact` and `FailInvoice` are real jobs the feature tests dispatch and work
with `queue:work --once`.

- `RecordingSink` is bound as `MetricSink` by the sampler tests. `value($name, $class)` returns what
  was published for that metric and class, and asserts it was published at most once.
- `SpyStore` records one entry per store round trip; it is how the hot-path cost is pinned.
- The counter store guard stands down under `runningUnitTests()`. Tests that need it set
  `app()['env'] = 'production'` and put `testing` back in `afterEach`, or the migration rollback
  would ask for confirmation.

## Conventions

- `declare(strict_types=1)` in every file.
- `env()` only in `config/`: the arch test forbids it elsewhere, and Larastan's `configDirectories`
  points at `config/`.
- Pint's Laravel preset, with opening braces on the same line (`pint.json`).

## Invariants

What the tests hold the code to. A change that breaks one needs a reason.

1. The listener never throws into the application.
2. A pair is drained only under a lock the run still holds, after the sink returned, by exactly
   what was read. Counters are never reset or deleted.
3. One dimension set per EMF document.
4. Per-class values add up to the queue total, and the kept classes do not change between windows.
5. A registered counter costs one round trip per event between re-check marks.
6. The counter store increments atomically, takes locks, and shares no connection with a Redis
   queue.

## Extending

- **A sink.** Implement `MetricSink::write()`, which gets one batch per pair and run, with raw
  dimension values. Throw when delivery fails: returning drains the counters. Register it with
  `MetricSinkManager::extend()`, or add a `create{Name}Driver()` method to the manager for a
  built-in one.
- **A gauge.** Add the name to `MetricName` and read it in `Collector::collect()`.
- **A counter.** Add it to `MetricName::COUNTERS`, which `Counters::read()` and
  `Collector::collect()` iterate, and increment it from `RecordJobMetrics`. Each counter adds a key
  per class to every read, and a custom metric per published class on CloudWatch.
