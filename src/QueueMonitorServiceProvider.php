<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use CodingDuck\QueueMonitor\Console\Commands\SampleCommand;
use CodingDuck\QueueMonitor\Console\Commands\StatusCommand;
use Illuminate\Cache\CacheManager;
use Illuminate\Cache\DynamoDbStore;
use Illuminate\Cache\RedisStore;
use Illuminate\Cache\Repository as CacheRepository;
use Illuminate\Contracts\Cache\Store;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobQueued;
use Illuminate\Support\ServiceProvider;
use RuntimeException;

class QueueMonitorServiceProvider extends ServiceProvider {
    public function register(): void {
        $this->mergeConfigFrom(__DIR__.'/../config/queue-monitor.php', 'queue-monitor');

        $this->app->singleton(QueueMonitor::class);
        $this->app->singleton(MetricSinkManager::class);
        $this->app->singleton(RecordJobMetrics::class);

        $this->app->singleton(MetricSink::class, fn (Application $app): MetricSink => $app->make(MetricSinkManager::class)->sink());

        $this->app->singleton(Counters::class, function (Application $app): Counters {
            $monitor = $app->make(QueueMonitor::class);
            $cache = $app->make(CacheManager::class);
            $name = $monitor->counterStore() ?? $cache->getDefaultDriver();
            $store = $cache->store($name)->getStore();

            $this->guardCounterStore($app, $store, $name);

            // A repository of its own, without events: the host's cache
            // listeners (Pulse, Telescope) have no business with counters.
            // The registry tracks a few times the published classes, so a
            // class past the cap still has room before it overflows.
            return new Counters(new CacheRepository($store), $monitor->counterPrefix(), 4 * $monitor->maxJobClasses());
        });
    }

    public function boot(): void {
        if ($this->app->make(QueueMonitor::class)->enabled()) {
            $events = $this->app->make(Dispatcher::class);

            $events->listen(JobQueued::class, [RecordJobMetrics::class, 'handleJobQueued']);
            $events->listen(JobProcessed::class, [RecordJobMetrics::class, 'handleJobProcessed']);
            $events->listen(JobFailed::class, [RecordJobMetrics::class, 'handleJobFailed']);
        }

        if ($this->app->runningInConsole()) {
            $this->publishes([__DIR__.'/../config/queue-monitor.php' => $this->app->configPath('queue-monitor.php')], 'queue-monitor-config');

            $this->commands([SampleCommand::class, StatusCommand::class]);
        }
    }

    /**
     * Counters must increment atomically across processes and take locks.
     */
    private function guardCounterStore(Application $app, Store $store, string $name): void {
        if ($app->runningUnitTests() || $store instanceof DynamoDbStore) {
            return;
        }

        if (! $store instanceof RedisStore) {
            throw new RuntimeException('The queue-monitor counter store must be a redis or dynamodb store; set queue-monitor.counters.store.');
        }

        $config = $app->make(Repository::class);
        $connection = $config->get("cache.stores.{$name}.connection");

        foreach ([self::redisName($connection), self::redisName($config->get("cache.stores.{$name}.lock_connection") ?? $connection)] as $redis) {
            if (! $config->has("database.redis.{$redis}") && ! $config->has("database.redis.clusters.{$redis}")) {
                throw new RuntimeException("The queue-monitor counter store uses the [{$redis}] Redis connection, which database.redis does not define.");
            }
        }
    }

    /**
     * RedisManager resolves an empty or missing name to the default connection.
     */
    private static function redisName(mixed $name): string {
        return is_string($name) && $name !== '' ? $name : 'default';
    }
}
