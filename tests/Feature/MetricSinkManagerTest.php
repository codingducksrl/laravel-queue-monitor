<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Metric;
use CodingDuck\QueueMonitor\MetricSink;
use CodingDuck\QueueMonitor\MetricSinkManager;
use CodingDuck\QueueMonitor\Sinks\EmfSink;

it('defaults to the EMF sink', function (?string $driver): void {
    config()->set('queue-monitor.sink', $driver);

    expect(app(MetricSinkManager::class)->sink())->toBeInstanceOf(EmfSink::class);
})->with(['emf', '', null]);

it('builds the EMF sink from the config', function (): void {
    config()->set('queue-monitor.emf.namespace', 'Acme/Queues');
    config()->set('queue-monitor.emf.entity', ['Service' => 'checkout', 'Environment' => null, 7 => 'x']);

    $sink = app(MetricSinkManager::class)->sink('emf');

    expect((fn (): array => [$this->namespace, $this->entity])->call($sink))->toBe(['Acme/Queues', ['Service' => 'checkout']]);
});

it('accepts a sink registered by the application', function (): void {
    $custom = new class implements MetricSink {
        /** @param list<Metric> $metrics */
        public function write(array $metrics): void {}
    };

    app(MetricSinkManager::class)->extend('datadog', fn (): MetricSink => $custom);

    expect(app(MetricSinkManager::class)->sink('datadog'))->toBe($custom);
});

it('rejects a driver that is not a sink', function (): void {
    app(MetricSinkManager::class)->extend('broken', fn (): stdClass => new stdClass);

    expect(fn (): MetricSink => app(MetricSinkManager::class)->sink('broken'))
        ->toThrow(InvalidArgumentException::class, MetricSink::class);
});

it('rejects an unknown driver', function (): void {
    expect(fn (): MetricSink => app(MetricSinkManager::class)->sink('nope'))
        ->toThrow(InvalidArgumentException::class, 'Driver [nope] not supported.');
});
