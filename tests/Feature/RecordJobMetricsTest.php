<?php

declare(strict_types=1);

use Aws\Sqs\SqsClient;
use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\MetricName;
use CodingDuck\QueueMonitor\RecordJobMetrics;
use Illuminate\Cache\ArrayStore;
use Illuminate\Cache\Repository;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobQueued;
use Illuminate\Queue\SqsQueue;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Exceptions;
use Illuminate\Support\Facades\Queue;
use Workbench\App\Jobs\FailInvoice;
use Workbench\App\Jobs\SendInvoice;

enum AuditQueue: string {
    case High = 'high';
}

// The guard stands down under unit tests; the migration rollback on teardown
// would ask for confirmation if the app were left in production.
afterEach(function (): void {
    app()['env'] = 'testing';
});

function readCounters(string $connection = 'database', string $queue = 'default'): array {
    return app(Counters::class)->read($connection, $queue);
}

function fakeJob(
    string $name = 'Workbench\App\Jobs\SendInvoice',
    ?string $queue = 'default',
    string $connection = 'database',
    bool $released = false,
    bool $failed = false,
): Job {
    $job = Mockery::mock(Job::class);
    $job->shouldReceive('getQueue')->andReturn($queue);
    $job->shouldReceive('getConnectionName')->andReturn($connection);
    $job->shouldReceive('payload')->andReturn(['displayName' => $name, 'job' => 'Illuminate\Queue\CallQueuedHandler@call']);
    $job->shouldReceive('isReleased')->andReturn($released);
    $job->shouldReceive('hasFailed')->andReturn($failed);

    return $job;
}

function queued(?string $queue = 'default', mixed $job = new SendInvoice, string $connection = 'database', mixed $enumQueue = null): JobQueued {
    return new JobQueued($connection, $enumQueue ?? $queue, 1, $job, '{', null);
}

function breakCounterStore(string $store = 'file'): void {
    app()['env'] = 'production';
    config()->set('queue-monitor.counters.store', $store);
    app()->forgetInstance(Counters::class);
}

it('counts a dispatched job against its connection, queue and class', function (): void {
    SendInvoice::dispatch()->onConnection('database');

    expect(readCounters())->toBe([
        MetricName::JobsQueued => ['Workbench\App\Jobs\SendInvoice' => 1],
    ]);
});

it('names a dispatched job without decoding its payload', function (): void {
    event(queued());

    expect(readCounters()[MetricName::JobsQueued])->toBe(['Workbench\App\Jobs\SendInvoice' => 1]);
});

it('uses the display name a job gives itself', function (): void {
    event(queued(job: new class {
        public function displayName(): string {
            return 'Nightly report';
        }
    }));

    expect(readCounters()[MetricName::JobsQueued])->toBe(['Nightly report' => 1]);
});

it('resolves a null or empty queue to the connection default', function (?string $queue): void {
    event(queued($queue));

    expect(readCounters()[MetricName::JobsQueued])->toBe(['Workbench\App\Jobs\SendInvoice' => 1]);
})->with([null, '']);

it('accepts an enum queue name', function (): void {
    config()->set('queue-monitor.queues', ['database' => ['high']]);

    event(queued(enumQueue: AuditQueue::High));

    expect(readCounters(queue: 'high')[MetricName::JobsQueued])->toBe(['Workbench\App\Jobs\SendInvoice' => 1]);
});

it('counts a processed job as completed', function (): void {
    event(new JobProcessed('database', fakeJob()));

    expect(readCounters()[MetricName::JobsCompleted])->toBe(['Workbench\App\Jobs\SendInvoice' => 1]);
});

it('does not count a released job as completed', function (): void {
    event(new JobProcessed('database', fakeJob(released: true)));

    expect(readCounters())->toBe([]);
});

it('counts a job that called fail() as failed only', function (): void {
    $job = fakeJob(failed: true);

    event(new JobProcessed('database', $job));
    event(new JobFailed('database', $job, new RuntimeException('nope')));

    expect(readCounters())->toBe([MetricName::JobsFailed => ['Workbench\App\Jobs\SendInvoice' => 1]]);
});

it('counts against the job connection rather than the worker connection', function (): void {
    event(new JobProcessed('failover', fakeJob()));

    expect(readCounters()[MetricName::JobsCompleted])->toBe(['Workbench\App\Jobs\SendInvoice' => 1]);
});

it('counts a job worked off the queue end to end', function (): void {
    SendInvoice::dispatch()->onConnection('database');

    $this->artisan('queue:work', ['connection' => 'database', '--once' => true])->assertSuccessful();

    expect(readCounters())->toBe([
        MetricName::JobsQueued => ['Workbench\App\Jobs\SendInvoice' => 1],
        MetricName::JobsCompleted => ['Workbench\App\Jobs\SendInvoice' => 1],
    ]);
});

it('counts nothing for a queue that is not sampled', function (): void {
    event(new JobProcessed('database', fakeJob(queue: 'high')));
    event(new JobProcessed('redis', fakeJob(connection: 'redis')));
    event(new JobProcessed('sync', fakeJob(queue: 'sync', connection: 'sync')));

    expect(readCounters())->toBe([])
        ->and(app(Counters::class)->classes('database', 'high'))->toBe([]);
});

it('counts a dispatch when it is pushed, inside a transaction or not', function (): void {
    DB::transaction(function (): void {
        event(queued());

        expect(readCounters()[MetricName::JobsQueued])->toBe(['Workbench\App\Jobs\SendInvoice' => 1]);
    });
});

it('names a queued closure the way the worker does', function (): void {
    Queue::connection('database')->push(function (): void {});

    $this->artisan('queue:work', ['connection' => 'database', '--once' => true])->assertSuccessful();

    $counters = readCounters();

    expect(array_keys($counters[MetricName::JobsQueued]))->toBe(array_keys($counters[MetricName::JobsCompleted]))
        ->and(array_key_first($counters[MetricName::JobsQueued]))->toStartWith('Closure (');
});

it('counts a job whose payload cannot be read under unknown without standing down', function (mixed $payload): void {
    Exceptions::fake();

    $poison = Mockery::mock(Job::class);
    $poison->shouldReceive('getQueue')->andReturn('default');
    $poison->shouldReceive('getConnectionName')->andReturn('database');
    $poison->shouldReceive('payload')->andReturn($payload);

    event(new JobFailed('database', $poison, new RuntimeException('bad payload')));
    event(new JobProcessed('database', fakeJob()));

    expect(readCounters())->toBe([
        MetricName::JobsFailed => ['unknown' => 1],
        MetricName::JobsCompleted => ['Workbench\App\Jobs\SendInvoice' => 1],
    ]);
    Exceptions::assertNothingReported();
})->with(['undecodable' => [null], 'not an object' => ['job'], 'nameless' => [['uuid' => 'u']]]);

it('caps the length of a job name', function (): void {
    event(new JobProcessed('database', fakeJob(str_repeat('n', 1000))));

    expect(array_key_first(readCounters()[MetricName::JobsCompleted]))->toHaveLength(255);
});

it('monitors a failover connection as the connection behind it', function (): void {
    config()->set('queue.default', 'failover');
    config()->set('queue.connections.failover', ['driver' => 'failover', 'connections' => ['database', 'sync']]);
    config()->set('queue-monitor.queues', []);

    SendInvoice::dispatch();
    event(new JobProcessed('failover', fakeJob()));

    expect(readCounters())->toBe([
        MetricName::JobsQueued => ['Workbench\App\Jobs\SendInvoice' => 1],
        MetricName::JobsCompleted => ['Workbench\App\Jobs\SendInvoice' => 1],
    ]);
});

it('maps an SQS worker queue URL back to the configured queue name', function (): void {
    app('queue')->extend('sqs-fake', fn (): ConnectorInterface => new class implements ConnectorInterface {
        public function connect(array $config): SqsQueue {
            return new SqsQueue(Mockery::mock(SqsClient::class), 'default', 'https://sqs.eu-west-1.amazonaws.com/123456789012/', '-production');
        }
    });
    config()->set('queue.connections.sqs', ['driver' => 'sqs-fake', 'queue' => 'default']);
    config()->set('queue-monitor.queues', ['sqs' => ['default']]);

    $url = 'https://sqs.eu-west-1.amazonaws.com/123456789012/default-production';

    event(queued(null, connection: 'sqs'));
    event(new JobProcessed('sqs', fakeJob(queue: $url, connection: 'sqs')));
    event(new JobFailed('sqs', fakeJob(queue: $url, connection: 'sqs'), new RuntimeException('nope')));

    expect(readCounters('sqs'))->toBe([
        MetricName::JobsQueued => ['Workbench\App\Jobs\SendInvoice' => 1],
        MetricName::JobsCompleted => ['Workbench\App\Jobs\SendInvoice' => 1],
        MetricName::JobsFailed => ['Workbench\App\Jobs\SendInvoice' => 1],
    ]);
});

it('counts a forwarded queue under the name it is monitored by', function (): void {
    app('queue.routes')->forward('default', 'overflow');

    event(queued());
    event(new JobProcessed('database', fakeJob(queue: 'overflow')));

    expect(readCounters())->toBe([
        MetricName::JobsQueued => ['Workbench\App\Jobs\SendInvoice' => 1],
        MetricName::JobsCompleted => ['Workbench\App\Jobs\SendInvoice' => 1],
    ]);
});

it('counts a job under the queue it landed on when the queue forwarded to it is monitored too', function (array $queues): void {
    config()->set('queue-monitor.queues', ['database' => $queues]);
    app('queue.routes')->forward('default', 'overflow');

    event(queued('overflow'));
    event(queued('default'));

    expect(readCounters(queue: 'overflow'))->toBe([MetricName::JobsQueued => ['Workbench\App\Jobs\SendInvoice' => 2]])
        ->and(readCounters())->toBe([]);
})->with([[['overflow', 'default']], [['default', 'overflow']]]);

it('counts every class as one when the class dimension is off', function (): void {
    config()->set('queue-monitor.max_job_classes', 0);
    app()->forgetInstance(Counters::class);

    event(queued());
    event(new JobProcessed('database', fakeJob('App\Jobs\Other')));

    expect(readCounters())->toBe([
        MetricName::JobsQueued => [Counters::OTHER => 1],
        MetricName::JobsCompleted => [Counters::OTHER => 1],
    ]);
});

it('counts a background connection under sync, which runs its jobs', function (): void {
    config()->set('queue.connections.background', ['driver' => 'background']);
    config()->set('queue-monitor.queues', ['background' => ['default']]);

    event(new JobProcessed('sync', fakeJob(queue: 'sync', connection: 'sync')));

    expect(readCounters('sync')[MetricName::JobsCompleted])->toBe(['Workbench\App\Jobs\SendInvoice' => 1]);
});

it('never lets a broken counter store reach the job', function (): void {
    Exceptions::fake();

    app()->instance(Counters::class, new Counters(new Repository(new class extends ArrayStore {
        public function increment($key, $value = 1) {
            throw new RuntimeException('cache is down');
        }
    }), 'qm-broken'));

    event(new JobProcessed('database', fakeJob()));

    Exceptions::assertReported(fn (RuntimeException $e): bool => $e->getMessage() === 'cache is down');
});

it('keeps dispatch working when reporting the failure throws too', function (): void {
    Exceptions::reportable(fn (Throwable $e): never => throw new RuntimeException('reporter is down'));
    breakCounterStore('does-not-exist');

    SendInvoice::dispatch()->onConnection('database');

    expect(DB::table('jobs')->count())->toBe(1);
});

it('keeps dispatch working when the counter store is rejected or undefined', function (string $store): void {
    Exceptions::fake();
    breakCounterStore($store);

    SendInvoice::dispatch()->onConnection('database');

    expect(DB::table('jobs')->count())->toBe(1);
    Exceptions::assertReportedCount(1);
})->with(['file', 'does-not-exist']);

it('still records a failed job when the counter store is broken', function (): void {
    Exceptions::fake();
    breakCounterStore('does-not-exist');

    FailInvoice::dispatch()->onConnection('database');

    $this->artisan('queue:work', ['connection' => 'database', '--once' => true]);

    expect(DB::table('failed_jobs')->count())->toBe(1);
});

it('stands down for a minute after a failure instead of retrying every event', function (): void {
    Exceptions::fake();
    breakCounterStore();

    event(queued());
    event(queued());
    Exceptions::assertReportedCount(1);

    $this->travel(61)->seconds();
    event(queued());
    Exceptions::assertReportedCount(2);
});

it('is one listener instance per process', function (): void {
    expect(app(RecordJobMetrics::class))->toBe(app(RecordJobMetrics::class));
});

it('counts jobs run on a deferred connection under its monitored queue', function (): void {
    config()->set('queue.connections.deferred', ['driver' => 'deferred']);
    config()->set('queue-monitor.queues', ['deferred' => ['default']]);

    event(new JobProcessed('deferred', fakeJob(queue: 'sync', connection: 'deferred')));

    expect(readCounters('deferred')[MetricName::JobsCompleted])->toBe(['Workbench\App\Jobs\SendInvoice' => 1]);
});
