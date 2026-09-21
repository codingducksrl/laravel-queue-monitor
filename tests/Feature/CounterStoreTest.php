<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\MetricName;
use CodingDuck\QueueMonitor\QueueMonitorServiceProvider;
use Illuminate\Cache\ArrayStore;
use Illuminate\Cache\FileStore;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\Facades\Cache;

it('counts on a store that refuses to increment a missing key', function (string $store): void {
    $counters = new Counters(Cache::store($store), 'qm-'.$store);

    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    expect($counters->read('redis', 'default', MetricName::counters()))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 2]]);
})->with(['database', 'array']);

it('drains a database backed counter without losing a concurrent increment', function (): void {
    $counters = new Counters(Cache::store('database'), 'qm-drain');

    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    $readings = $counters->read('redis', 'default', MetricName::counters());

    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    $counters->commit('redis', 'default', $readings);

    expect($counters->read('redis', 'default', MetricName::counters()))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1]]);
});

it('refuses a cache store that cannot increment atomically across processes', function (string $store): void {
    expect(fn (): mixed => guardStore(new $store(...guardArguments($store))))
        ->toThrow(RuntimeException::class, 'increment atomically');
})->with([ArrayStore::class, FileStore::class]);

it('accepts a cache store that does', function (): void {
    expect(guardStore(Cache::store('database')->getStore()))->toBeNull();
});

it('stands down while running tests', function (): void {
    $app = Mockery::mock(Application::class);
    $app->shouldReceive('runningUnitTests')->andReturn(true);

    expect(guardStore(new ArrayStore, $app))->toBeNull();
});

function guardArguments(string $store): array {
    return $store === FileStore::class ? [app('files'), sys_get_temp_dir()] : [];
}

function guardStore(object $store, ?object $app = null): mixed {
    if ($app === null) {
        $app = Mockery::mock(Application::class);
        $app->shouldReceive('runningUnitTests')->andReturn(false);
    }

    $provider = new QueueMonitorServiceProvider(app());

    return (new ReflectionMethod($provider, 'guardCounterStore'))->invoke($provider, $app, $store);
}
