<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Console\Commands;

use CodingDuck\QueueMonitor\QueueMonitor;
use Illuminate\Console\Command;

class QueueMonitorCommand extends Command {
    /**
     * @var string
     */
    protected $signature = 'queue-monitor:status';

    /**
     * @var string
     */
    protected $description = 'Show the current queue monitoring configuration';

    public function handle(QueueMonitor $monitor): int {
        if (! $monitor->enabled()) {
            $this->components->warn('Queue monitoring is disabled.');

            return self::SUCCESS;
        }

        $connections = $monitor->connections();

        $this->components->info(
            $connections === []
                ? 'Queue monitoring is enabled for every connection.'
                : 'Queue monitoring is enabled for: '.implode(', ', $connections).'.'
        );

        return self::SUCCESS;
    }
}
