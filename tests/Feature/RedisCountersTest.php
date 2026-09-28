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
        'host' => getenv('REDIS_HOST') ?: '127.0.0.1',
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
    if (! extension_loaded('redis') && ! getenv('CI')) {
        $this->markTestSkipped('The redis extension is not installed.');
    }
});

it('counts, registers and drains on redis', function (array $options): void {
    $counters = redisCounters($options);

    foreach (range(1, 3) as $ignored) {
        $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    }

    $readings = $counters->read('redis', 'default', MetricName::counters());
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $counters->commit('redis', 'default', $readings);

    expect($readings)->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 3]])
        ->and($counters->classes('redis', 'default'))->toBe(['App\Jobs\A'])
        ->and($counters->read('redis', 'default', MetricName::counters()))
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
