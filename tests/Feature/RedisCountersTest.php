<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\MetricName;
use Illuminate\Cache\Repository;
use Illuminate\Support\Facades\Cache;

/**
 * Runs against a real server: CI provides one and fails without it, while
 * locally it is skipped unless REDIS_HOST points at one.
 */
function redisCounters(array $options = []): Counters {
    config()->set('database.redis.qm', [
        'host' => getenv('REDIS_HOST'),
        'port' => (int) (getenv('REDIS_PORT') ?: 6379),
        'database' => 15,
        'options' => $options,
    ]);
    config()->set('cache.stores.qm', ['driver' => 'redis', 'connection' => 'qm', 'lock_connection' => 'qm']);

    try {
        app('redis')->connection('qm')->flushdb();
    } catch (Throwable $e) {
        getenv('CI') ? throw $e : test()->markTestSkipped('No Redis server is reachable.');
    }

    return new Counters(new Repository(Cache::store('qm')->getStore()), 'qm-redis');
}

beforeEach(function (): void {
    if (! getenv('CI') && (! extension_loaded('redis') || ! getenv('REDIS_HOST'))) {
        $this->markTestSkipped('Needs the redis extension and REDIS_HOST.');
    }
});

it('counts, registers and drains on redis', function (array $options): void {
    $counters = redisCounters($options);

    foreach (range(1, 3) as $ignored) {
        $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    }

    $readings = $counters->read('redis', 'default');
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $counters->commit('redis', 'default', $readings);

    expect($readings)->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 3]])
        ->and($counters->classes('redis', 'default'))->toBe(['App\Jobs\A'])
        ->and($counters->read('redis', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1]]);
})->with(function (): array {
    // Serialized or compressed values used to poison the seed, so INCRBY failed forever.
    $sets = ['plain' => [[]], 'php serializer' => [['serializer' => 1]]];

    foreach (['LZF', 'LZ4', 'ZSTD'] as $algorithm) {
        if (defined("Redis::COMPRESSION_{$algorithm}")) {
            $sets[strtolower($algorithm).' compression'] = [['compression' => constant("Redis::COMPRESSION_{$algorithm}")]];
        }
    }

    return $sets;
});
