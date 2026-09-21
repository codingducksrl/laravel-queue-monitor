<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Facades\QueueMonitor as QueueMonitorFacade;
use CodingDuck\QueueMonitor\QueueMonitor;

it('binds the monitor as a singleton', function (): void {
    expect(app(QueueMonitor::class))->toBe(app(QueueMonitor::class));
});

it('merges the package config', function (): void {
    expect(config('queue-monitor.enabled'))->toBeTrue()
        ->and(config('queue-monitor.connections'))->toBe([]);
});

it('resolves the facade', function (): void {
    expect(QueueMonitorFacade::monitors('redis'))->toBeTrue();
});

it('registers the status command', function (): void {
    $this->artisan('queue-monitor:status')
        ->expectsOutputToContain('every connection')
        ->assertSuccessful();
});
