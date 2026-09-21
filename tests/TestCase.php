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
     * SQLite ignores foreign keys unless the pragma is on, which would let
     * every referential constraint in the package migrations pass untested.
     * Testbench falls back to the `testing` connection when no sqlite file
     * exists, so target whichever connection is actually the default.
     *
     * @param Application $app
     */
    protected function defineEnvironment($app): void {
        // The encrypter needs a key; the test app ships none.
        $app['config']->set('app.key', 'base64:'.base64_encode(str_repeat('a', 32)));

        $connection = $app['config']->get('database.default');

        $app['config']->set("database.connections.{$connection}.foreign_key_constraints", true);

        // The failed job provider otherwise points at a sqlite file that the
        // test environment never creates.
        $app['config']->set('queue.failed.database', $connection);
    }

    /**
     * The jobs, job_batches and failed_jobs tables the queue drivers and the
     * failed job provider are exercised against.
     */
    protected function defineDatabaseMigrationsAfterDatabaseRefreshed(): void {
        $this->loadLaravelMigrations();
    }
}
