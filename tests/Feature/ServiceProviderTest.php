<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Facades\QueueMonitor as QueueMonitorFacade;
use CodingDuck\QueueMonitor\QueueMonitor;
use Illuminate\Console\Scheduling\Schedule;
use Monolog\Handler\StreamHandler;

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

it('resolves the facade', function (): void {
    expect(QueueMonitorFacade::sampledQueues())->toBe([['database', 'default']]);
});

it('rejects an EMF namespace CloudWatch would drop', function (string $namespace): void {
    config()->set('queue-monitor.sink', 'emf');
    config()->set('queue-monitor.emf.namespace', $namespace);

    $this->artisan('queue-monitor:status')->expectsOutputToContain('EMF namespace')->assertFailed();
})->with(['AWS/SQS', 'Queues!', ':Queues']);

it('rejects an EMF log channel that would swallow the documents', function (array $channel, string $message): void {
    config()->set('logging.channels.emf', $channel);
    config()->set('queue-monitor.sink', 'emf');
    config()->set('queue-monitor.emf.channel', 'emf');

    $this->artisan('queue-monitor:status')->expectsOutputToContain($message)->assertFailed();
})->with([
    'undefined stack member' => [['driver' => 'stack', 'channels' => ['nowhere']], '[nowhere] is not defined'],
    'unbuildable driver' => [['driver' => 'bogus'], '[emf] is not defined or could not be built'],
    'level above info' => [['driver' => 'single', 'path' => '/dev/null', 'level' => 'warning'], 'straight through'],
    'level above info in handler_with' => [[
        'driver' => 'monolog', 'handler' => StreamHandler::class, 'handler_with' => ['stream' => 'php://memory', 'level' => 'warning'],
    ], 'straight through'],
    'fingers crossed' => [['driver' => 'single', 'path' => '/dev/null', 'action_level' => 'error'], 'straight through'],
    'ignored failures' => [['driver' => 'stack', 'channels' => ['single'], 'ignore_exceptions' => true], 'straight through'],
    'null handler first' => [['driver' => 'stack', 'channels' => ['null', 'single']], 'straight through'],
]);

it('accepts a working EMF log channel', function (): void {
    config()->set('logging.channels.emf', ['driver' => 'single', 'path' => sys_get_temp_dir().'/qm-emf.log', 'level' => 'info']);
    config()->set('queue-monitor.sink', 'emf');
    config()->set('queue-monitor.emf.channel', 'emf');

    $this->artisan('queue-monitor:status')->assertSuccessful();
});

it('schedules as the README says without sending the output to /dev/null', function (): void {
    $event = app(Schedule::class)->command('queue-monitor:sample')
        ->everyMinute()
        ->onOneServer()
        ->runInBackground()
        ->appendOutputTo('/proc/1/fd/1');

    // The background wrapper still silences schedule:finish; the command itself must not be.
    expect($event->buildCommand())->toContain("queue-monitor:sample >> '/proc/1/fd/1' 2>&1");
});

it('registers the status command', function (): void {
    $this->artisan('queue-monitor:status')
        ->expectsOutputToContain('database:default')
        ->assertSuccessful();
});

it('fails the status check when the counter store cannot be used', function (): void {
    app()['env'] = 'production';
    config()->set('queue-monitor.counters.store', 'file');

    $this->artisan('queue-monitor:status')
        ->expectsOutputToContain('increment atomically')
        ->assertFailed();
});

it('fails the status check when the EMF log channel is not defined', function (): void {
    config()->set('queue-monitor.sink', 'emf');
    config()->set('queue-monitor.emf.channel', 'nowhere');

    $this->artisan('queue-monitor:status')
        ->expectsOutputToContain('[nowhere] is not defined')
        ->assertFailed();
});
