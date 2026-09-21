<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Collector;
use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\MetricName;
use CodingDuck\QueueMonitor\MetricSink;
use CodingDuck\QueueMonitor\Tests\Support\RecordingSink;
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

    expect($this->sink->value(MetricName::JobsCompleted))->toBeNull();
});

it('leaves the counters alone on a dry run', function (): void {
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    $this->artisan('queue-monitor:sample', ['--dry-run' => true])->assertSuccessful();

    expect($this->sink->metrics)->toBeEmpty()
        ->and(app(Counters::class)->read('database', 'default', MetricName::counters()))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1]]);
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
