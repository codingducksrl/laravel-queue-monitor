<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Metric;
use CodingDuck\QueueMonitor\MetricSink;
use CodingDuck\QueueMonitor\MetricSinkManager;
use CodingDuck\QueueMonitor\Sinks\Emf\EmfSink;
use CodingDuck\QueueMonitor\Sinks\NullSink;

it('resolves the configured sink', function (string $driver, string $expected): void {
    config()->set('queue-monitor.sink', $driver);

    expect(app(MetricSinkManager::class)->sink())->toBeInstanceOf($expected);
})->with([
    'emf' => ['emf', EmfSink::class],
    'null' => ['null', NullSink::class],
]);

it('falls back to the null sink when none is configured', function (): void {
    config()->set('queue-monitor.sink', '');

    expect(app(MetricSinkManager::class)->sink())->toBeInstanceOf(NullSink::class);
});

it('memoises the resolved sink', function (): void {
    $manager = app(MetricSinkManager::class);

    expect($manager->sink())->toBe($manager->sink());
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
