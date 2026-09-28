<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Collector;
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
use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobQueued;
use Illuminate\Queue\Failed\DatabaseUuidFailedJobProvider;
use Illuminate\Queue\Failed\NullFailedJobProvider;
use Illuminate\Queue\NullQueue;
use Illuminate\Support\Carbon;
use Illuminate\Support\Collection as IlluminateCollection;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Exceptions;
use Illuminate\Support\Facades\Queue;
use Workbench\App\Jobs\SendInvoice;
use Workbench\App\Jobs\SyncContact;

beforeEach(function (): void {
    config()->set('queue-monitor.queues', ['database' => ['default']]);

    $this->sink = new RecordingSink;
    app()->instance(MetricSink::class, $this->sink);
});

it('reads the queue depth gauges straight from the driver', function (): void {
    Queue::connection('database')->push(new SendInvoice);
    Queue::connection('database')->push(new SendInvoice);
    Queue::connection('database')->later(600, new SyncContact);
    Queue::connection('database')->pop('default');

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::JobsPending))->toBe(1)
        ->and($this->sink->value(MetricName::JobsDelayed))->toBe(1)
        ->and($this->sink->value(MetricName::JobsInProgress))->toBe(1);
});

it('reports the stored failed job total', function (): void {
    app('queue.failer')->log('database', 'default', json_encode(['uuid' => 'a', 'displayName' => 'J']), new RuntimeException('nope'));

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::FailedJobsTotal))->toBe(1);
});

it('breaks in-progress jobs down by class', function (): void {
    Queue::connection('database')->push(new SendInvoice);
    Queue::connection('database')->pop('default');

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::JobsInProgress, 'Workbench\App\Jobs\SendInvoice'))->toBe(1);
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
        ->and(app(Counters::class)->read('database', 'default', MetricName::counters()))
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

    $counters = app(Counters::class);

    foreach (['A' => 100, 'B' => 50, 'C' => 5, 'D' => 3] as $class => $times) {
        foreach (range(1, $times) as $ignored) {
            $counters->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\\'.$class);
        }
    }

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->classesFor(MetricName::JobsCompleted))
        ->toBe(['App\Jobs\A', 'App\Jobs\B', Collector::OTHER])
        ->and($this->sink->value(MetricName::JobsCompleted, Collector::OTHER))->toBe(8)
        ->and($this->sink->value(MetricName::JobsCompleted))->toBe(158);
});

it('keeps the same classes across windows, busy or not', function (): void {
    config()->set('queue-monitor.max_job_classes', 1);

    $counters = app(Counters::class);
    $counters->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $this->artisan('queue-monitor:sample')->assertSuccessful();

    $this->sink->metrics = [];
    $counters->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\B');
    $counters->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\B');
    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->classesFor(MetricName::JobsCompleted))->toBe([Collector::OTHER])
        ->and($this->sink->value(MetricName::JobsCompleted, Collector::OTHER))->toBe(2);
});

it('folds the in-progress breakdown into the same classes', function (): void {
    config()->set('queue-monitor.max_job_classes', 1);

    app(Counters::class)->increment('database', 'default', MetricName::JobsQueued, 'App\Jobs\A');
    Queue::connection('database')->push(new SendInvoice);
    Queue::connection('database')->pop('default');

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->classesFor(MetricName::JobsInProgress))->toBe([Collector::OTHER]);
});

it('publishes a job class named like a number as a string', function (): void {
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, '42');

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::JobsCompleted, '42'))->toBe(1)
        ->and(app(Counters::class)->read('database', 'default', MetricName::counters()))->toBe([]);
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

    expect(app(Counters::class)->read('database', 'default', MetricName::counters()))
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

it('publishes nothing while monitoring is disabled', function (): void {
    config()->set('queue-monitor.enabled', false);

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->metrics)->toBeEmpty();
});

function sampleLock(string $connection = 'database', string $queue = 'default'): string {
    return 'queue-monitor:sample:'.hash('xxh128', $connection."\0".$queue);
}

function useCounterStore(): void {
    config()->set('cache.stores.counters', ['driver' => 'array']);
    config()->set('queue-monitor.counters.store', 'counters');
    app()->forgetInstance(Counters::class);
    app()->forgetInstance(Collector::class);
}

it('takes the sampler lock in the counter store rather than the default one', function (): void {
    useCounterStore();
    $lock = Cache::store('counters')->getStore()->lock(sampleLock(), 60);
    $lock->get();

    $this->artisan('queue-monitor:sample')->expectsOutputToContain('skipping')->assertSuccessful();

    expect($this->sink->metrics)->toBeEmpty();
});

it('keeps the sampler lock well past a normal read and refreshes it per queue', function (): void {
    useCounterStore();
    Carbon::setTestNow('2026-09-28 12:00:00');

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
    Carbon::setTestNow();
});

it('does not drain a queue whose lock expired while it was being read', function (): void {
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
    app()->forgetInstance(Collector::class);

    $this->artisan('queue-monitor:sample')->expectsOutputToContain('lost the sampler lock')->assertFailed();

    expect($this->sink->metrics)->toBeEmpty()
        ->and(app(Counters::class)->read('database', 'default', MetricName::counters()))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1]]);
    Carbon::setTestNow();
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

it('publishes no throughput for a pair that is not counted', function (): void {
    $this->artisan('queue-monitor:sample', ['queues' => 'database:high'])->assertSuccessful();

    expect($this->sink->value(MetricName::JobsPending))->toBe(0)
        ->and($this->sink->value(MetricName::JobsCompleted))->toBeNull();
});

it('publishes no failed job total for a provider that only answers zero', function (bool $behindCloud): void {
    $failer = new NullFailedJobProvider;
    app()->instance('queue.failer', $behindCloud ? new CloudFailedJobProvider($failer, new CloudEvents('127.0.0.1:1'), app('encrypter')) : $failer);
    app()->forgetInstance(Collector::class);

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::FailedJobsTotal))->toBeNull();
})->with(['null' => false, 'null behind Laravel Cloud' => true]);

it('counts failed jobs of a forwarded queue under the queue they landed on', function (): void {
    app('queue.routes')->forward('default', 'overflow');
    app('queue.failer')->log('database', 'overflow', json_encode(['uuid' => 'a', 'displayName' => 'J']), new RuntimeException('nope'));

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::FailedJobsTotal))->toBe(1);
});

it('scans the database jobs table once for jobs in progress', function (): void {
    Queue::connection('database')->push(new SendInvoice);
    Queue::connection('database')->pop('default');
    DB::enableQueryLog();

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    $scans = array_filter(DB::getQueryLog(), fn (array $query): bool => str_contains($query['query'], '"jobs"'));

    expect($scans)->toHaveCount(3)
        ->and($this->sink->value(MetricName::JobsInProgress))->toBe(1);
});

it('still samples a queue holding a reserved job it cannot inspect', function (): void {
    DB::table('jobs')->insert([
        'queue' => 'default', 'payload' => 'not json', 'attempts' => 1,
        'reserved_at' => time(), 'available_at' => time(), 'created_at' => time(),
    ]);

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::JobsInProgress))->toBe(1)
        ->and($this->sink->classesFor(MetricName::JobsInProgress))->toBe([]);
});

it('prints only what would be published on a dry run', function (): void {
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, "\u{009B}31mEvil\u{202E}");

    $this->artisan('queue-monitor:sample', ['--dry-run' => true])
        ->expectsOutputToContain('JobClass=31mEvil#')
        ->doesntExpectOutputToContain("\u{009B}")
        ->assertSuccessful();
});

it('publishes no queued total for a connection that never fires JobQueued', function (): void {
    config()->set('queue-monitor.queues', ['sync' => ['default']]);

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::JobsQueued))->toBeNull()
        ->and($this->sink->value(MetricName::JobsCompleted))->toBe(0);
});

it('publishes no failed job total for Laravel Cloud managed queues', function (): void {
    config()->set('queue.connections.cloud', ['driver' => 'database', 'table' => 'jobs', 'queue' => 'default']);
    config()->set('queue-monitor.queues', ['cloud' => ['default']]);
    app()->instance('queue.failer', new CloudFailedJobProvider(app('queue.failer'), new CloudEvents('127.0.0.1:1'), app('encrypter')));
    app()->forgetInstance(Collector::class);

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::FailedJobsTotal))->toBeNull();
});

it('counts failed jobs a Redis worker stored under the forwarded queue name', function (): void {
    app('queue.routes')->forward('default', 'overflow');
    app('queue.failer')->log('database', 'default', json_encode(['uuid' => 'a', 'displayName' => 'J']), new RuntimeException('nope'));

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::FailedJobsTotal))->toBe(1);
});

it('does not fetch reserved payloads when nothing is reserved', function (): void {
    $driver = new class extends NullQueue {
        public int $fetches = 0;

        public function reservedJobs($queue = null): IlluminateCollection {
            $this->fetches++;

            return new IlluminateCollection;
        }
    };
    app('queue')->extend('spy', fn () => new class($driver) implements ConnectorInterface {
        public function __construct(private NullQueue $driver) {}

        public function connect(array $config): NullQueue {
            return $this->driver;
        }
    });
    config()->set('queue.connections.spy', ['driver' => 'spy']);
    config()->set('queue-monitor.queues', ['spy' => ['default']]);

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($driver->fetches)->toBe(0);
});

it('maps a failover connection on the command line to the connection behind it', function (): void {
    config()->set('queue.default', 'failover');
    config()->set('queue.connections.failover', ['driver' => 'failover', 'connections' => ['database', 'sync']]);
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    $this->artisan('queue-monitor:sample', ['queues' => 'default'])->assertSuccessful();

    expect($this->sink->metrics[0]->dimensions['Connection'])->toBe('database')
        ->and($this->sink->value(MetricName::JobsCompleted))->toBe(1);
});

it('reports a lock that cannot be released without failing the run', function (): void {
    Exceptions::fake();
    config()->set('cache.stores.counters', ['driver' => 'counters-broken-release']);
    Cache::extend('counters-broken-release', fn (): Repository => new Repository(new class extends ArrayStore {
        public function lock($name, $seconds = 0, $owner = null) {
            return new class($this, $name, $seconds, $owner) extends ArrayLock {
                public function release() {
                    throw new RuntimeException('release failed');
                }
            };
        }
    }));
    config()->set('queue-monitor.counters.store', 'counters');
    app()->forgetInstance(Counters::class);
    app()->forgetInstance(Collector::class);

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    Exceptions::assertReported(fn (RuntimeException $e): bool => $e->getMessage() === 'release failed');
});

it('names in-progress jobs the way the counters do', function (): void {
    $long = str_repeat('n', 300);
    event(new JobQueued('database', 'default', 1, new class($long) {
        public function __construct(private string $name) {}

        public function displayName(): string {
            return $this->name;
        }
    }, '{', null));
    DB::table('jobs')->insert([
        'queue' => 'default', 'payload' => json_encode(['uuid' => 'u', 'displayName' => $long, 'job' => 'x', 'data' => []]),
        'attempts' => 1, 'reserved_at' => time(), 'available_at' => time(), 'created_at' => time(),
    ]);

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->classesFor(MetricName::JobsInProgress))->toBe([str_repeat('n', 255)]);
});

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
    app()->forgetInstance(Collector::class);
}

it('drains when a refresh changes nothing but the lock is still held', function (): void {
    // MySQL reports an UPDATE to the same expiry, within the same second, as zero rows.
    useLockStore(fn ($store, $name, $seconds, $owner) => new class($store, $name, $seconds, $owner) extends ArrayLock {
        public function refresh($seconds = null) {
            return false;
        }
    });
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    expect($this->sink->value(MetricName::JobsCompleted))->toBe(1)
        ->and(app(Counters::class)->read('database', 'default', MetricName::counters()))->toBe([]);
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

it('counts a sync-family connection towards its first queue only', function (): void {
    config()->set('queue.connections.deferred', ['driver' => 'deferred']);
    config()->set('queue-monitor.queues', ['deferred' => ['default', 'high']]);

    event(new JobProcessed('deferred', Mockery::mock(Job::class, [
        'getQueue' => 'sync', 'getConnectionName' => 'deferred', 'resolveName' => 'App\Jobs\A',
        'isReleased' => false, 'hasFailed' => false,
    ])));

    expect(app(Counters::class)->read('deferred', 'default', MetricName::counters()))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1]])
        ->and(app(Counters::class)->read('deferred', 'high', MetricName::counters()))->toBe([]);
});
