<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use CodingDuck\QueueMonitor\Console\Commands\QueueMonitorCommand;
use CodingDuck\QueueMonitor\Console\Commands\SampleCommand;
use Illuminate\Cache\ArrayStore;
use Illuminate\Cache\FileStore;
use Illuminate\Contracts\Cache\Factory as CacheFactory;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
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
                return new QueueMonitor($app->make(Repository::class));
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
                $cache = $app->make(CacheFactory::class)->store($monitor->counterStore());

                $this->guardCounterStore($app, $cache->getStore());

                return new Counters($cache, $monitor->counterPrefix());
            }
        );

        $this->app->singleton(
            Collector::class, function (Application $app): Collector {
                return new Collector(
                    $app->make(QueueFactory::class),
                    $app->make(Counters::class),
                    $app->make('queue.failer'),
                    $app->make(QueueMonitor::class)->maxJobClasses(),
                );
            }
        );
    }

    /**
     * Bootstrap any package services.
     */
    public function boot(): void {
        $this->loadMigrationsFrom(__DIR__.'/../database/migrations');

        if ($this->app->make(QueueMonitor::class)->enabled()) {
            $this->listenForJobEvents();
        }

        if (! $this->app->runningInConsole()) {
            return;
        }

        $this->publishes(
            [
                __DIR__.'/../config/queue-monitor.php' => config_path('queue-monitor.php'),
            ], ['queue-monitor', 'queue-monitor-config']
        );

        $this->publishes(
            [
                __DIR__.'/../database/migrations' => database_path('migrations'),
            ], ['queue-monitor', 'queue-monitor-migrations']
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
     * Counters must increment atomically across processes. An array store is a
     * per-process buffer and a file store is not atomic under concurrency, so
     * either would quietly lose counts.
     */
    private function guardCounterStore(Application $app, object $store): void {
        if ($app->runningUnitTests()) {
            return;
        }

        if ($store instanceof ArrayStore || $store instanceof FileStore) {
            throw new RuntimeException(
                'The queue-monitor counter store must increment atomically across processes; '
                .'set queue-monitor.counters.store to a redis, memcached, database or dynamodb store.'
            );
        }
    }
}
