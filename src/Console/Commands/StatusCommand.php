<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Console\Commands;

use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\MetricSink;
use CodingDuck\QueueMonitor\MetricSinkManager;
use CodingDuck\QueueMonitor\QueueMonitor;
use Illuminate\Cache\CacheManager;
use Illuminate\Console\Command;
use Illuminate\Contracts\Queue\Factory;
use Throwable;

final class StatusCommand extends Command {
    protected $signature = 'queue-monitor:status';

    protected $description = 'Show the queue monitoring configuration and check that it can be built';

    public function handle(QueueMonitor $monitor): int {
        if (! $monitor->enabled()) {
            $this->components->warn('Queue monitoring is disabled.');

            return self::SUCCESS;
        }

        try {
            $pairs = $monitor->sampledQueues();

            foreach ($pairs as [$connection]) {
                $this->laravel->make(Factory::class)->connection($connection);
            }

            $this->laravel->make(Counters::class);
            $this->laravel->make(MetricSink::class);
        } catch (Throwable $e) {
            $this->components->error($e->getMessage());

            return self::FAILURE;
        }

        $max = $monitor->maxJobClasses();

        $this->components->info('Queue monitoring is enabled.');
        $this->components->twoColumnDetail('Sink', $this->laravel->make(MetricSinkManager::class)->getDefaultDriver());
        $this->components->twoColumnDetail('Counter store', $monitor->counterStore() ?? $this->laravel->make(CacheManager::class)->getDefaultDriver());
        $this->components->twoColumnDetail('JobClass dimension', $max > 0 ? "first {$max} classes" : 'off');

        foreach ($pairs as [$connection, $queue]) {
            $this->components->twoColumnDetail('Monitored queue', "{$connection}:{$queue}");
        }

        return self::SUCCESS;
    }
}
