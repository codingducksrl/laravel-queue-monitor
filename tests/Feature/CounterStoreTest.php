<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\MetricName;
use CodingDuck\QueueMonitor\QueueMonitorServiceProvider;
use Illuminate\Cache\ArrayStore;
use Illuminate\Cache\DatabaseStore;
use Illuminate\Cache\FileStore;
use Illuminate\Cache\NullStore;
use Illuminate\Cache\RedisStore;
use Illuminate\Contracts\Cache\Store;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;

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

it('refuses a cache store that cannot count across processes', function (string $store): void {
    expect(fn (): mixed => guardStore(new $store(...guardArguments($store))))
        ->toThrow(RuntimeException::class, 'increment atomically');
})->with([ArrayStore::class, FileStore::class, NullStore::class]);

it('refuses a database store on the application default connection', function (): void {
    expect(fn (): mixed => guardStore(Cache::store('database')->getStore()))
        ->toThrow(RuntimeException::class, 'other than the default');
});

it('accepts a database store on a connection of its own', function (): void {
    config()->set('database.connections.counters', ['driver' => 'sqlite', 'database' => ':memory:']);

    expect(guardStore(new DatabaseStore(DB::connection('counters'), 'cache')))->toBeNull();
});

it('accepts a redis store', function (): void {
    expect(guardStore(new RedisStore(app('redis'))))->toBeNull();
});

it('stands down while running tests', function (): void {
    $app = Mockery::mock(Application::class);
    $app->shouldReceive('runningUnitTests')->andReturn(true);

    expect(guardStore(new ArrayStore, $app))->toBeNull();
});

function guardArguments(string $store): array {
    return $store === FileStore::class ? [app('files'), sys_get_temp_dir()] : [];
}

function guardStore(Store $store, ?object $app = null): mixed {
    if ($app === null) {
        $app = Mockery::mock(app())->makePartial();
        $app->shouldReceive('runningUnitTests')->andReturn(false);
    }

    $provider = new QueueMonitorServiceProvider(app());

    return (new ReflectionMethod($provider, 'guardCounterStore'))->invoke($provider, $app, $store);
}

it('keeps counter traffic out of the application cache events', function (): void {
    $fired = [];
    Event::listen('Illuminate\Cache\Events\*', function (string $event) use (&$fired): void {
        $fired[] = $event;
    });

    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    app(Counters::class)->read('database', 'default', MetricName::counters());

    expect($fired)->toBe([]);
});
