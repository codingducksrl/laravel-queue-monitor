<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use CodingDuck\QueueMonitor\Console\Commands\QueueMonitorCommand;
use CodingDuck\QueueMonitor\Console\Commands\SampleCommand;
use Illuminate\Cache\DatabaseStore;
use Illuminate\Cache\DynamoDbStore;
use Illuminate\Cache\MemcachedStore;
use Illuminate\Cache\RedisStore;
use Illuminate\Cache\Repository as CacheRepository;
use Illuminate\Contracts\Cache\Factory as CacheFactory;
use Illuminate\Contracts\Cache\Store;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Database\DatabaseManager;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobQueued;
use Illuminate\Support\ServiceProvider;
use RuntimeException;

class QueueMonitorServiceProvider extends ServiceProvider {
    /**
     * Register any package services.
     */
    public function register(): void {
        $this->mergeConfigFrom(__DIR__.'/../config/queue-monitor.php', 'queue-monitor');

        $this->app->singleton(
            QueueMonitor::class, function (Application $app): QueueMonitor {
                return new QueueMonitor($app->make(Repository::class), $app);
            }
        );

        $this->app->singleton(
            MetricSinkManager::class, function (Application $app): MetricSinkManager {
                return new MetricSinkManager($app);
            }
        );

        $this->app->singleton(
            MetricSink::class, function (Application $app): MetricSink {
                return $app->make(MetricSinkManager::class)->sink();
            }
        );

        $this->app->singleton(
            Counters::class, function (Application $app): Counters {
                $monitor = $app->make(QueueMonitor::class);
                $store = $app->make(CacheFactory::class)->store($monitor->counterStore())->getStore();

                $this->guardCounterStore($app, $store);

                // A repository of its own, without events: the host's cache
                // listeners (Pulse, Telescope) have no business with counters.
                // The registry tracks a few times the published classes, so a
                // class past the cap still has room before it overflows.
                return new Counters(new CacheRepository($store), $monitor->counterPrefix(), 4 * $monitor->maxJobClasses());
            }
        );

        $this->app->singleton(
            Collector::class, function (Application $app): Collector {
                return new Collector(
                    $app->make(QueueFactory::class),
                    $app->make(Counters::class),
                    $app->make('queue.failer'),
                    $app->make(ExceptionHandler::class),
                    $app->make(QueueMonitor::class),
                    $app->make(QueueMonitor::class)->maxJobClasses(),
                );
            }
        );

        $this->app->singleton(RecordJobMetrics::class);
    }

    /**
     * Bootstrap any package services.
     */
    public function boot(): void {
        if ($this->app->make(QueueMonitor::class)->enabled()) {
            $this->listenForJobEvents();
        }

        if (! $this->app->runningInConsole()) {
            return;
        }

        $this->publishes(
            [
                __DIR__.'/../config/queue-monitor.php' => $this->app->configPath('queue-monitor.php'),
            ], ['queue-monitor', 'queue-monitor-config']
        );

        $this->commands(
            [
                QueueMonitorCommand::class,
                SampleCommand::class,
            ]
        );
    }

    private function listenForJobEvents(): void {
        $events = $this->app->make(Dispatcher::class);

        $events->listen(JobQueued::class, [RecordJobMetrics::class, 'handleJobQueued']);
        $events->listen(JobProcessed::class, [RecordJobMetrics::class, 'handleJobProcessed']);
        $events->listen(JobFailed::class, [RecordJobMetrics::class, 'handleJobFailed']);
    }

    /**
     * Counters must increment atomically across processes and take locks. A
     * database store on the application's own connection would put a locking
     * transaction on its primary database for every job, so it must have a
     * connection of its own. Anything else would quietly lose counts.
     */
    private function guardCounterStore(Application $app, Store $store): void {
        if ($app->runningUnitTests()) {
            return;
        }

        $supported = match (true) {
            $store instanceof RedisStore, $store instanceof MemcachedStore, $store instanceof DynamoDbStore => true,
            $store instanceof DatabaseStore => $store->getConnection() !== $app->make(DatabaseManager::class)->connection(),
            default => false,
        };

        if (! $supported) {
            throw new RuntimeException(
                'The queue-monitor counter store must increment atomically across processes; '
                .'set queue-monitor.counters.store to a redis, memcached or dynamodb store, '
                .'or a database store on a connection other than the default one.'
            );
        }
    }
}
