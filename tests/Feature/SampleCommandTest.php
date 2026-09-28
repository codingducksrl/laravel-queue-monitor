<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\MetricName;
use CodingDuck\QueueMonitor\MetricSink;
use CodingDuck\QueueMonitor\Tests\Support\RecordingSink;
use Illuminate\Cache\ArrayLock;
use Illuminate\Cache\ArrayStore;
use Illuminate\Cache\Repository;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Foundation\Cloud\Events as CloudEvents;
use Illuminate\Foundation\Cloud\FailedJobProvider as CloudFailedJobProvider;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Failed\DatabaseUuidFailedJobProvider;
use Illuminate\Queue\Failed\NullFailedJobProvider;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Exceptions;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Facades\Schema;
use Workbench\App\Jobs\SendInvoice;
use Workbench\App\Jobs\SyncContact;

beforeEach(function (): void {
    config()->set('queue-monitor.queues', ['database' => ['default']]);

    $this->sink = new RecordingSink;
    app()->instance(MetricSink::class, $this->sink);
});

function sampleLock(string $connection = 'database', string $queue = 'default'): string {
    return 'queue-monitor:sample:'.hash('xxh128', $connection."\0".$queue);
}

function useCounterStore(): void {
    config()->set('cache.stores.counters', ['driver' => 'array']);
    config()->set('queue-monitor.counters.store', 'counters');
    app()->forgetInstance(Counters::class);
}

function useLockStore(Closure $lock): void {
    Cache::extend('qm-locks', fn (): Repository => new Repository(new class($lock) extends ArrayStore {
        public function __construct(private Closure $make) {
            parent::__construct();
        }

        public function lock($name, $seconds = 0, $owner = null) {
            return ($this->make)($this, $name, $seconds, $owner);
        }
    }));
    config()->set('cache.stores.counters', ['driver' => 'qm-locks']);
    config()->set('queue-monitor.counters.store', 'counters');
    app()->forgetInstance(Counters::class);
}

it('reads the queue depth gauges straight from the driver', function (): void {
    Queue::connection('database')->push(new SendInvoice);
    Queue::connection('database')->push(new SendInvoice);
    Queue::connection('database')->later(600, new SyncContact);
    Queue::connection('database')->pop('default');

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::JobsPending))->toBe(1)
        ->and($this->sink->value(MetricName::JobsDelayed))->toBe(1)
        ->and($this->sink->value(MetricName::JobsInProgress))->toBe(1)
        ->and($this->sink->classesFor(MetricName::JobsInProgress))->toBe([]);
});

it('reads the depth of a database queue with three counts and no payloads', function (): void {
    Queue::connection('database')->push(new SendInvoice);
    Queue::connection('database')->pop('default');
    DB::enableQueryLog();

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    $queries = array_column(array_filter(DB::getQueryLog(), fn (array $query): bool => str_contains($query['query'], '"jobs"')), 'query');

    expect($queries)->toHaveCount(3)->each->toContain('count(*)');
});

it('reports the stored failed job total', function (): void {
    app('queue.failer')->log('database', 'default', json_encode(['uuid' => 'a', 'displayName' => 'J']), new RuntimeException('nope'));

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::FailedJobsTotal))->toBe(1);
});

it('still publishes the depth when failed jobs cannot be counted', function (): void {
    Exceptions::fake();
    Schema::drop('failed_jobs');
    Queue::connection('database')->push(new SendInvoice);

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::FailedJobsTotal))->toBeNull()
        ->and($this->sink->value(MetricName::JobsPending))->toBe(1);
    Exceptions::assertReportedCount(1);
});

it('publishes each counter at both the queue and the class level', function (): void {
    $counters = app(Counters::class);
    $counters->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $counters->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $counters->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\B');

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::JobsCompleted))->toBe(3)
        ->and($this->sink->value(MetricName::JobsCompleted, 'App\Jobs\A'))->toBe(2)
        ->and($this->sink->value(MetricName::JobsCompleted, 'App\Jobs\B'))->toBe(1);
});

it('publishes the queue totals only when the class dimension is off', function (): void {
    config()->set('queue-monitor.max_job_classes', 0);
    app()->forgetInstance(Counters::class);

    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\B');

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::JobsCompleted))->toBe(2)
        ->and($this->sink->classesFor(MetricName::JobsCompleted))->toBe([])
        ->and(app(Counters::class)->read('database', 'default'))->toBe([]);
});

it('drains each counter exactly once', function (): void {
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    $this->artisan('queue-monitor:sample')->assertSuccessful();
    expect($this->sink->value(MetricName::JobsCompleted))->toBe(1);

    $this->sink->metrics = [];
    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::JobsCompleted))->toBe(0);
});

it('leaves the counters alone on a dry run', function (): void {
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    $this->artisan('queue-monitor:sample', ['--dry-run' => true])->assertSuccessful();

    expect($this->sink->metrics)->toBeEmpty()
        ->and(app(Counters::class)->read('database', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1]]);
});

it('publishes idle throughput as zero at the queue level only', function (): void {
    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::JobsQueued))->toBe(0)
        ->and($this->sink->value(MetricName::JobsCompleted))->toBe(0)
        ->and($this->sink->value(MetricName::JobsFailed))->toBe(0)
        ->and($this->sink->classesFor(MetricName::JobsCompleted))->toBe([]);
});

it('folds job classes past the cap while keeping the queue total intact', function (): void {
    config()->set('queue-monitor.max_job_classes', 2);
    app()->forgetInstance(Counters::class);

    $counters = app(Counters::class);

    foreach (['A' => 100, 'B' => 50, 'C' => 5, 'D' => 3] as $class => $times) {
        foreach (range(1, $times) as $ignored) {
            $counters->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\\'.$class);
        }
    }

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->classesFor(MetricName::JobsCompleted))
        ->toBe(['App\Jobs\A', 'App\Jobs\B', Counters::OTHER])
        ->and($this->sink->value(MetricName::JobsCompleted, Counters::OTHER))->toBe(8)
        ->and($this->sink->value(MetricName::JobsCompleted))->toBe(158);
});

it('gives the overflow bucket no class slot after the breakdown is turned back on', function (): void {
    config()->set('queue-monitor.max_job_classes', 0);
    app()->forgetInstance(Counters::class);
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $this->artisan('queue-monitor:sample')->assertSuccessful();

    config()->set('queue-monitor.max_job_classes', 1);
    app()->forgetInstance(Counters::class);
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $this->sink->metrics = [];
    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->classesFor(MetricName::JobsCompleted))->toBe(['App\Jobs\A']);
});

it('keeps the same classes across windows, busy or not', function (): void {
    config()->set('queue-monitor.max_job_classes', 1);
    app()->forgetInstance(Counters::class);

    $counters = app(Counters::class);
    $counters->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $this->artisan('queue-monitor:sample')->assertSuccessful();

    $this->sink->metrics = [];
    $counters->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\B');
    $counters->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\B');
    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->classesFor(MetricName::JobsCompleted))->toBe([Counters::OTHER])
        ->and($this->sink->value(MetricName::JobsCompleted, Counters::OTHER))->toBe(2);
});

it('publishes a job class named like a number as a string', function (): void {
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, '42');

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::JobsCompleted, '42'))->toBe(1)
        ->and(app(Counters::class)->read('database', 'default'))->toBe([]);
});

it('samples the remaining queues when one of them fails', function (): void {
    config()->set('queue-monitor.queues', ['missing' => ['default'], 'database' => ['default']]);
    Queue::connection('database')->push(new SendInvoice);

    $this->artisan('queue-monitor:sample')->assertFailed();

    expect($this->sink->value(MetricName::JobsPending))->toBe(1);
});

it('keeps the counters when the sink fails', function (): void {
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    app()->instance(MetricSink::class, new class implements MetricSink {
        public function write(array $metrics): void {
            throw new RuntimeException('sink is down');
        }
    });

    $this->artisan('queue-monitor:sample')->assertFailed();

    expect(app(Counters::class)->read('database', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1]]);
});

it('skips while another sampler holds the lock on the counter store', function (): void {
    $lock = Cache::store()->getStore()->lock(sampleLock(), 60);
    $lock->get();

    $this->artisan('queue-monitor:sample')->expectsOutputToContain('skipping')->assertSuccessful();

    expect($this->sink->metrics)->toBeEmpty();

    $lock->release();
});

it('does not take the lock for a dry run', function (): void {
    $lock = Cache::store()->getStore()->lock(sampleLock(), 60);
    $lock->get();

    $this->artisan('queue-monitor:sample', ['--dry-run' => true])
        ->expectsOutputToContain('JobsPending')
        ->assertSuccessful();

    $lock->release();
});

it('ignores empty and duplicate entries in the queue argument', function (): void {
    $this->artisan('queue-monitor:sample', ['queues' => 'database:high,,database:high,'])->assertSuccessful();

    expect(array_filter($this->sink->metrics, fn ($m): bool => $m->name === MetricName::JobsPending))->toHaveCount(1);
});

it('rejects a queue argument without a connection or a queue', function (string $argument): void {
    $this->artisan('queue-monitor:sample', ['queues' => $argument])->assertFailed();
})->with([':high', 'database:']);

it('accepts an explicit connection and queue pair', function (): void {
    Queue::connection('database')->pushOn('high', new SendInvoice);

    $this->artisan('queue-monitor:sample', ['queues' => 'database:high'])->assertSuccessful();

    expect($this->sink->value(MetricName::JobsPending))->toBe(1)
        ->and($this->sink->metrics[0]->dimensions)->toBe(['Connection' => 'database', 'Queue' => 'high']);
});

it('publishes nothing and builds no counter store while monitoring is disabled', function (): void {
    config()->set('queue-monitor.enabled', false);
    config()->set('queue-monitor.counters.store', 'missing');
    app()->forgetInstance(Counters::class);

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->metrics)->toBeEmpty();
});

it('takes the sampler lock in the counter store rather than the default one', function (): void {
    useCounterStore();
    $lock = Cache::store('counters')->getStore()->lock(sampleLock(), 60);
    $lock->get();

    $this->artisan('queue-monitor:sample')->expectsOutputToContain('skipping')->assertSuccessful();

    expect($this->sink->metrics)->toBeEmpty();
});

it('refreshes the sampler lock before draining a queue', function (): void {
    useCounterStore();
    Carbon::setTestNow('2026-09-28 12:00:00');

    // Reading the queue takes 200 of the lock's 300 seconds.
    DB::listen(fn () => Carbon::setTestNow('2026-09-28 12:03:20'));

    $seen = [];
    app()->instance(MetricSink::class, new class($seen) implements MetricSink {
        public function __construct(private array &$seen) {}

        public function write(array $metrics): void {
            $lock = (fn (): array => $this->locks[sampleLock()])->call(Cache::store('counters')->getStore());
            $this->seen[] = $lock['expiresAt']->getTimestamp() - now()->getTimestamp();
        }
    });

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($seen)->toBe([300]);
});

it('does not drain a queue whose lock expired while it was being read', function (): void {
    Exceptions::fake();
    useCounterStore();
    Carbon::setTestNow('2026-09-28 12:00:00');
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    // A failed job count that takes longer than the lock, after which another run takes over.
    app()->instance('queue.failer', new class(app('db'), config('database.default'), 'failed_jobs') extends DatabaseUuidFailedJobProvider {
        public function count($connection = null, $queue = null): int {
            Carbon::setTestNow(now()->addSeconds(301));
            Cache::store('counters')->lock(sampleLock(), 60)->get();

            return 0;
        }
    });

    $this->artisan('queue-monitor:sample')->expectsOutputToContain('Lost the sampler lock')->assertFailed();

    expect($this->sink->metrics)->toBeEmpty()
        ->and(app(Counters::class)->read('database', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1]]);
});

it('samples the other queues while one is held by another run', function (): void {
    useCounterStore();
    config()->set('queue-monitor.queues', ['database' => ['default', 'high']]);
    Cache::store('counters')->getStore()->lock(sampleLock(), 60)->get();
    Queue::connection('database')->pushOn('high', new SendInvoice);

    $this->artisan('queue-monitor:sample')->expectsOutputToContain('database:default is being sampled')->assertSuccessful();

    expect($this->sink->value(MetricName::JobsPending))->toBe(1)
        ->and($this->sink->metrics[0]->dimensions['Queue'])->toBe('high');
});

it('samples the other queues when taking one lock fails', function (): void {
    config()->set('queue-monitor.queues', ['database' => ['default', 'high']]);
    useLockStore(fn ($store, $name, $seconds, $owner) => new class($store, $name, $seconds, $owner) extends ArrayLock {
        public function acquire() {
            return $this->name === sampleLock() ? throw new RuntimeException('lock store timed out') : parent::acquire();
        }
    });
    Queue::connection('database')->pushOn('high', new SendInvoice);

    $this->artisan('queue-monitor:sample')->expectsOutputToContain('lock store timed out')->assertFailed();

    expect($this->sink->value(MetricName::JobsPending))->toBe(1);
});

it('fails the run and reports a lock that cannot be released', function (): void {
    Exceptions::fake();
    useLockStore(fn ($store, $name, $seconds, $owner) => new class($store, $name, $seconds, $owner) extends ArrayLock {
        public function release() {
            throw new RuntimeException('release failed');
        }
    });

    $this->artisan('queue-monitor:sample')->assertFailed();

    Exceptions::assertReported(fn (RuntimeException $e): bool => $e->getMessage() === 'release failed');
});

it('publishes no throughput for a pair that is not counted', function (): void {
    $this->artisan('queue-monitor:sample', ['queues' => 'database:high'])->assertSuccessful();

    expect($this->sink->value(MetricName::JobsPending))->toBe(0)
        ->and($this->sink->value(MetricName::JobsCompleted))->toBeNull();
});

it('publishes no failed job total for a provider that only answers zero', function (bool $behindCloud): void {
    $failer = new NullFailedJobProvider;
    app()->instance('queue.failer', $behindCloud ? new CloudFailedJobProvider($failer, new CloudEvents('127.0.0.1:1'), app('encrypter')) : $failer);

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::FailedJobsTotal))->toBeNull();
})->with(['null' => false, 'null behind Laravel Cloud' => true]);

it('counts the failed jobs of the provider Laravel Cloud wraps', function (): void {
    app('queue.failer')->log('database', 'default', json_encode(['uuid' => 'a', 'displayName' => 'J']), new RuntimeException('nope'));
    app()->instance('queue.failer', new CloudFailedJobProvider(app('queue.failer'), new CloudEvents('127.0.0.1:1'), app('encrypter')));

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::FailedJobsTotal))->toBe(1);
});

it('publishes no failed job total for Laravel Cloud managed queues', function (): void {
    config()->set('queue.connections.cloud', ['driver' => 'database', 'table' => 'jobs', 'queue' => 'default']);
    config()->set('queue-monitor.queues', ['cloud' => ['default']]);
    app()->instance('queue.failer', new CloudFailedJobProvider(app('queue.failer'), new CloudEvents('127.0.0.1:1'), app('encrypter')));

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::FailedJobsTotal))->toBeNull();
});

it('counts failed jobs of a forwarded queue under the queue they landed on', function (): void {
    app('queue.routes')->forward('default', 'overflow');
    app('queue.failer')->log('database', 'overflow', json_encode(['uuid' => 'a', 'displayName' => 'J']), new RuntimeException('nope'));

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::FailedJobsTotal))->toBe(1);
});

it('counts failed jobs a Redis worker stored under the forwarded queue name', function (): void {
    app('queue.routes')->forward('default', 'overflow');
    app('queue.failer')->log('database', 'default', json_encode(['uuid' => 'a', 'displayName' => 'J']), new RuntimeException('nope'));

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::FailedJobsTotal))->toBe(1);
});

it('prints only what would be published on a dry run', function (): void {
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, "\u{009B}31mEvil\u{202E}");

    $this->artisan('queue-monitor:sample', ['--dry-run' => true])
        ->expectsOutputToContain('JobClass=31mEvil#')
        ->doesntExpectOutputToContain("\u{009B}")
        ->assertSuccessful();
});

it('prints console style tags in a job name as text on a dry run', function (): void {
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\<fg=black;bg=black>Hidden</> [x].');

    $this->artisan('queue-monitor:sample', ['--dry-run' => true])
        ->expectsOutputToContain('JobClass=App\Jobs\<fg=black;bg=black>Hidden</> [x]. 1')
        ->assertSuccessful();
});

it('counts the failed jobs of a sync connection towards its first queue', function (): void {
    config()->set('queue-monitor.queues', ['sync' => ['default', 'high']]);
    app('queue.failer')->log('sync', 'sync', json_encode(['uuid' => 'a', 'displayName' => 'J']), new RuntimeException('nope'));

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    $failed = array_values(array_filter($this->sink->metrics, fn ($m): bool => $m->name === MetricName::FailedJobsTotal));

    expect(array_map(fn ($m): array => [$m->dimensions['Queue'], $m->value], $failed))->toBe([['default', 1], ['high', 0]]);
});

it('publishes no queued total for a connection that never fires JobQueued', function (): void {
    config()->set('queue-monitor.queues', ['sync' => ['default']]);

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::JobsQueued))->toBeNull()
        ->and($this->sink->value(MetricName::JobsCompleted))->toBe(0);
});

it('maps a failover connection on the command line to the connection behind it', function (): void {
    config()->set('queue.default', 'failover');
    config()->set('queue.connections.failover', ['driver' => 'failover', 'connections' => ['database', 'sync']]);
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    $this->artisan('queue-monitor:sample', ['queues' => 'default'])->assertSuccessful();

    expect($this->sink->metrics[0]->dimensions['Connection'])->toBe('database')
        ->and($this->sink->value(MetricName::JobsCompleted))->toBe(1);
});

it('counts a sync-family connection towards its first queue only', function (): void {
    config()->set('queue.connections.deferred', ['driver' => 'deferred']);
    config()->set('queue-monitor.queues', ['deferred' => ['default', 'high']]);

    event(new JobProcessed('deferred', Mockery::mock(Job::class, [
        'getQueue' => 'sync', 'getConnectionName' => 'deferred', 'payload' => ['displayName' => 'App\Jobs\A'],
        'isReleased' => false, 'hasFailed' => false,
    ])));

    expect(app(Counters::class)->read('deferred', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1]])
        ->and(app(Counters::class)->read('deferred', 'high'))->toBe([]);
});
