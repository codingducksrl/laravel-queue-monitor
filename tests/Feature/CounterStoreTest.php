<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\MetricName;
use Illuminate\Cache\DynamoDbStore;
use Illuminate\Cache\Repository;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Event;

// The guard stands down under unit tests; the migration rollback on teardown
// would ask for confirmation if the app were left in production.
afterEach(function (): void {
    app()['env'] = 'testing';
});

function counterStore(array $store): Counters {
    app()['env'] = 'production';
    config()->set('database.redis.metrics', ['host' => '127.0.0.1']);
    config()->set('database.redis.cache', ['host' => '127.0.0.1']);
    config()->set('cache.stores.counters', $store);
    config()->set('queue-monitor.counters.store', 'counters');
    app()->forgetInstance(Counters::class);

    return app(Counters::class);
}

it('refuses a cache store other than redis or dynamodb', function (array $store): void {
    expect(fn (): Counters => counterStore($store))->toThrow(RuntimeException::class, 'redis or dynamodb');
})->with([
    'array' => [['driver' => 'array']],
    'file' => [['driver' => 'file', 'path' => sys_get_temp_dir()]],
    'null' => [['driver' => 'null']],
    'database' => [['driver' => 'database', 'table' => 'cache']],
]);

it('accepts a redis store with a connection of its own', function (): void {
    expect(counterStore(['driver' => 'redis', 'connection' => 'metrics', 'lock_connection' => 'metrics']))
        ->toBeInstanceOf(Counters::class);
});

it('accepts a redis store that shares a connection with a redis queue', function (array $store): void {
    config()->set('queue.connections.redis', ['driver' => 'redis', 'connection' => 'default', 'queue' => 'default']);

    expect(counterStore($store))->toBeInstanceOf(Counters::class);
})->with([
    'stock defaults' => [['driver' => 'redis', 'connection' => 'cache', 'lock_connection' => 'default']],
    'same connection' => [['driver' => 'redis']],
    'empty names' => [['driver' => 'redis', 'connection' => '', 'lock_connection' => '']],
]);

it('refuses a redis store whose connection is not defined', function (array $store): void {
    expect(fn (): Counters => counterStore($store))
        ->toThrow(RuntimeException::class, '[metircs] Redis connection, which database.redis does not define');
})->with([
    'connection' => [['driver' => 'redis', 'connection' => 'metircs']],
    'lock connection' => [['driver' => 'redis', 'connection' => 'metrics', 'lock_connection' => 'metircs']],
]);

it('keeps four times the published classes in the registry', function (): void {
    config()->set('queue-monitor.max_job_classes', 1);

    $counters = app(Counters::class);

    foreach (['A', 'B', 'C', 'D', 'E'] as $class) {
        $counters->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\\'.$class);
    }

    expect($counters->classes('database', 'default'))->toBe(['App\Jobs\A', 'App\Jobs\B', 'App\Jobs\C', 'App\Jobs\D', Counters::OTHER]);
});

it('accepts a dynamodb store', function (): void {
    Cache::extend('fake-dynamodb', fn (): Repository => new Repository(Mockery::mock(DynamoDbStore::class)));

    expect(counterStore(['driver' => 'fake-dynamodb']))->toBeInstanceOf(Counters::class);
});

it('stands down while running tests', function (): void {
    config()->set('queue-monitor.counters.store', 'array');

    expect(app(Counters::class))->toBeInstanceOf(Counters::class);
});

it('keeps counter traffic out of the application cache events', function (): void {
    $fired = [];
    Event::listen('Illuminate\Cache\Events\*', function (string $event) use (&$fired): void {
        $fired[] = $event;
    });

    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    app(Counters::class)->read('database', 'default');

    expect($fired)->toBe([]);
});
