<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use CodingDuck\QueueMonitor\Console\Commands\QueueMonitorCommand;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\ServiceProvider;

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
    }

    /**
     * Bootstrap any package services.
     */
    public function boot(): void {
        $this->loadMigrationsFrom(__DIR__.'/../database/migrations');

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
            ]
        );
    }
}
