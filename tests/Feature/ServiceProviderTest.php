<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\QueueMonitor;
use CodingDuck\QueueMonitor\QueueMonitorServiceProvider;
use Illuminate\Contracts\Events\Dispatcher as DispatcherContract;
use Illuminate\Events\Dispatcher;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobQueued;

afterEach(function (): void {
    app()['env'] = 'testing';
});

it('binds the monitor as a singleton', function (): void {
    expect(app(QueueMonitor::class))->toBe(app(QueueMonitor::class));
});

it('merges the package config, switched off by default', function (): void {
    $defaults = require __DIR__.'/../../config/queue-monitor.php';

    expect($defaults['enabled'])->toBeFalse()
        ->and(config('queue-monitor.counters.prefix'))->toBe('queue-monitor');
});

it('observes queue events only while monitoring is enabled', function (bool $enabled): void {
    config()->set('queue-monitor.enabled', $enabled);
    app()->instance(DispatcherContract::class, $events = new Dispatcher(app()));

    (new QueueMonitorServiceProvider(app()))->boot();

    foreach ([JobQueued::class, JobProcessed::class, JobFailed::class] as $event) {
        expect($events->hasListeners($event))->toBe($enabled);
    }
})->with([true, false]);

it('rejects an EMF namespace CloudWatch would drop', function (string $namespace): void {
    config()->set('queue-monitor.emf.namespace', $namespace);

    $this->artisan('queue-monitor:status')->expectsOutputToContain('EMF namespace')->assertFailed();
})->with(['AWS/SQS', 'Queues!', ':Queues']);

it('registers the status command', function (): void {
    config()->set('cache.default', 'array');

    $this->artisan('queue-monitor:status')
        ->expectsOutputToContain('database:default')
        ->expectsOutputToContain('array')
        ->assertSuccessful();
});

it('shows when the job class dimension is off', function (): void {
    config()->set('queue-monitor.max_job_classes', 0);

    $this->artisan('queue-monitor:status')->expectsOutputToContain('off')->assertSuccessful();
});

it('fails the status check when the counter store cannot be used', function (): void {
    app()['env'] = 'production';
    config()->set('queue-monitor.counters.store', 'file');

    $this->artisan('queue-monitor:status')
        ->expectsOutputToContain('redis or dynamodb')
        ->assertFailed();
});

it('fails the status check when a monitored connection is not configured', function (): void {
    config()->set('queue-monitor.queues', ['redsi' => ['default']]);

    $this->artisan('queue-monitor:status')
        ->expectsOutputToContain('[redsi] queue connection has not been configured')
        ->assertFailed();
});
