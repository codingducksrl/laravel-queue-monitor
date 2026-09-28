<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Console\Commands;

use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\MetricSink;
use CodingDuck\QueueMonitor\QueueMonitor;
use Illuminate\Console\Command;
use Throwable;

class QueueMonitorCommand extends Command {
    /**
     * @var string
     */
    protected $signature = 'queue-monitor:status';

    /**
     * @var string
     */
    protected $description = 'Show the queue monitoring configuration and check that it can run';

    /**
     * Exits non-zero when the counter store or the sink cannot be built, so a
     * deploy can run it as a readiness check before traffic arrives.
     */
    public function handle(QueueMonitor $monitor): int {
        if (! $monitor->enabled()) {
            $this->components->warn('Queue monitoring is disabled.');

            return self::SUCCESS;
        }

        try {
            $pairs = $monitor->sampledQueues();
            $this->laravel->make(Counters::class);
            $this->laravel->make(MetricSink::class);
        } catch (Throwable $e) {
            $this->components->error($e->getMessage());

            return self::FAILURE;
        }

        $this->components->info('Queue monitoring is enabled.');
        $this->components->twoColumnDetail('Sink', $monitor->sink());
        $this->components->twoColumnDetail('Counter store', $monitor->counterStore() ?? 'default');
        $this->components->twoColumnDetail('Job class cap', (string) $monitor->maxJobClasses());

        foreach ($pairs as [$connection, $queue]) {
            $this->components->twoColumnDetail('Monitored queue', "{$connection}:{$queue}");
        }

        return self::SUCCESS;
    }
}
