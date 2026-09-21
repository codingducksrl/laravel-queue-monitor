<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\MetricName;
use Illuminate\Cache\ArrayStore;
use Illuminate\Cache\Repository;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobQueued;
use Workbench\App\Jobs\SendInvoice;

function readCounters(string $connection = 'database', string $queue = 'default'): array {
    return app(Counters::class)->read($connection, $queue, MetricName::counters());
}

function fakeJob(string $name = 'Workbench\App\Jobs\SendInvoice', string $queue = 'default'): Job {
    $job = Mockery::mock(Job::class);
    $job->shouldReceive('getQueue')->andReturn($queue);
    $job->shouldReceive('resolveName')->andReturn($name);

    return $job;
}

it('counts a dispatched job against its connection, queue and class', function (): void {
    SendInvoice::dispatch()->onConnection('database');

    expect(readCounters())->toBe([
        MetricName::JobsQueued => ['Workbench\App\Jobs\SendInvoice' => 1],
    ]);
});

it('resolves a null queue to the connection default', function (): void {
    event(new JobQueued('database', null, 1, new SendInvoice, json_encode([
        'displayName' => 'Workbench\App\Jobs\SendInvoice',
    ]), null));

    expect(readCounters()[MetricName::JobsQueued])->toBe(['Workbench\App\Jobs\SendInvoice' => 1]);
});

it('counts a processed job as completed', function (): void {
    event(new JobProcessed('database', fakeJob()));

    expect(readCounters()[MetricName::JobsCompleted])->toBe(['Workbench\App\Jobs\SendInvoice' => 1]);
});

it('counts a failed job separately from a completed one', function (): void {
    event(new JobFailed('database', fakeJob(), new RuntimeException('nope')));

    expect(readCounters())->toBe([
        MetricName::JobsFailed => ['Workbench\App\Jobs\SendInvoice' => 1],
    ]);
});

it('counts a job worked off the queue end to end', function (): void {
    SendInvoice::dispatch()->onConnection('database');

    $this->artisan('queue:work', ['connection' => 'database', '--once' => true])->assertSuccessful();

    expect(readCounters())->toBe([
        MetricName::JobsQueued => ['Workbench\App\Jobs\SendInvoice' => 1],
        MetricName::JobsCompleted => ['Workbench\App\Jobs\SendInvoice' => 1],
    ]);
});

it('records nothing for a connection outside the configured list', function (): void {
    config()->set('queue-monitor.connections', ['redis']);

    event(new JobProcessed('database', fakeJob()));

    expect(readCounters())->toBe([]);
});

it('records nothing while monitoring is disabled', function (): void {
    config()->set('queue-monitor.enabled', false);

    event(new JobProcessed('database', fakeJob()));

    expect(readCounters())->toBe([]);
});

it('never lets a broken counter store reach the job', function (): void {
    app()->forgetInstance(Counters::class);
    app()->bind(Counters::class, fn (): Counters => new Counters(
        new Repository(new class extends ArrayStore {
            public function increment($key, $value = 1) {
                throw new RuntimeException('cache is down');
            }
        }),
        'qm-broken',
    ));

    event(new JobProcessed('database', fakeJob()));
})->throwsNoExceptions();
