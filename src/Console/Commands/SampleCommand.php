<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Console\Commands;

use CodingDuck\QueueMonitor\Collector;
use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\Metric;
use CodingDuck\QueueMonitor\MetricSink;
use CodingDuck\QueueMonitor\QueueMonitor;
use Illuminate\Console\Command;
use Illuminate\Contracts\Cache\Factory as CacheFactory;
use Illuminate\Contracts\Cache\LockProvider;

class SampleCommand extends Command {
    /**
     * @var string
     */
    protected $signature = 'queue-monitor:sample
        {queues? : Comma separated connection:queue pairs, defaulting to the configured ones}
        {--dry-run : Print the metrics without publishing or draining them}
        {--force : Run even if another sampler holds the lock}';

    /**
     * @var string
     */
    protected $description = 'Sample queue depth and publish the accumulated queue metrics';

    public function handle(
        QueueMonitor $monitor,
        Collector $collector,
        Counters $counters,
        MetricSink $sink,
        CacheFactory $cache,
    ): int {
        if (! $monitor->enabled()) {
            $this->components->warn('Queue monitoring is disabled.');

            return self::SUCCESS;
        }

        $store = $cache->store()->getStore();

        if ($this->option('force') || ! $store instanceof LockProvider) {
            return $this->sample($monitor, $collector, $counters, $sink);
        }

        $lock = $store->lock('queue-monitor:sample', 55);

        if (! $lock->get()) {
            $this->components->info('Another sampler holds the lock; skipping.');

            return self::SUCCESS;
        }

        try {
            return $this->sample($monitor, $collector, $counters, $sink);
        } finally {
            $lock->release();
        }
    }

    private function sample(QueueMonitor $monitor, Collector $collector, Counters $counters, MetricSink $sink): int {
        $dry = $this->option('dry-run') === true;

        foreach ($this->pairs($monitor) as [$connection, $queue]) {
            [$metrics, $readings] = $collector->collect($connection, $queue);

            if ($dry) {
                $this->render($metrics);

                continue;
            }

            $sink->write($metrics);
            $counters->commit($connection, $queue, $readings);
        }

        return self::SUCCESS;
    }

    /**
     * @return list<array{0: string, 1: string}>
     */
    private function pairs(QueueMonitor $monitor): array {
        $argument = $this->argument('queues');

        if (! is_string($argument) || $argument === '') {
            return $monitor->sampledQueues();
        }

        $pairs = [];

        foreach (explode(',', $argument) as $entry) {
            [$connection, $queue] = array_pad(explode(':', trim($entry), 2), 2, null);

            if ($queue === null) {
                $queue = $connection;
                $connection = $monitor->defaultConnection();
            }

            $pairs[] = [(string) $connection, (string) $queue];
        }

        return $pairs;
    }

    /**
     * @param list<Metric> $metrics
     */
    private function render(array $metrics): void {
        foreach ($metrics as $metric) {
            $dimensions = [];

            foreach ($metric->dimensions as $name => $value) {
                $dimensions[] = "{$name}={$value}";
            }

            $this->components->twoColumnDetail(
                $metric->name.' '.implode(' ', $dimensions),
                $metric->value.' '.$metric->unit->value,
            );
        }
    }
}
