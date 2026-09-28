<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Tests;

use CodingDuck\QueueMonitor\QueueMonitorServiceProvider;
use Illuminate\Foundation\Application;
use Orchestra\Testbench\TestCase as Orchestra;

abstract class TestCase extends Orchestra {
    /**
     * @return list<class-string>
     */
    protected function getPackageProviders($app): array {
        return [
            QueueMonitorServiceProvider::class,
        ];
    }

    /**
     * @param Application $app
     */
    protected function defineEnvironment($app): void {
        // Laravel Cloud's failed job provider needs an encrypter, and the test app ships no key.
        $app['config']->set('app.key', 'base64:'.base64_encode(str_repeat('a', 32)));

        // The failed job provider otherwise points at a sqlite file that the
        // test environment never creates.
        $app['config']->set('queue.failed.database', $app['config']->get('database.default'));

        $app['config']->set('queue-monitor.enabled', true);
        $app['config']->set('queue-monitor.queues', ['database' => ['default']]);
    }

    /**
     * The jobs, job_batches and failed_jobs tables the queue drivers and the
     * failed job provider are exercised against.
     */
    protected function defineDatabaseMigrationsAfterDatabaseRefreshed(): void {
        $this->loadLaravelMigrations();
    }
}
