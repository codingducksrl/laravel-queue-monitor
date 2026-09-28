<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Console\Commands;

use Closure;
use CodingDuck\QueueMonitor\Collector;
use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\Metric;
use CodingDuck\QueueMonitor\MetricSink;
use CodingDuck\QueueMonitor\QueueMonitor;
use CodingDuck\QueueMonitor\Sinks\Emf\EmfDocument;
use Illuminate\Cache\Lock as CacheLock;
use Illuminate\Console\Command;
use Illuminate\Contracts\Cache\Factory as CacheFactory;
use Illuminate\Contracts\Cache\Lock;
use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Throwable;

class SampleCommand extends Command {
    /**
     * Well above any sane read of one queue, and refreshed before that queue
     * is drained. A run holds one queue at a time, so a killed run delays
     * only that queue, and only until the lock expires.
     */
    private const int LOCK_SECONDS = 300;

    /**
     * @var string
     */
    protected $signature = 'queue-monitor:sample
        {queues? : Comma separated connection:queue pairs, defaulting to the configured ones}
        {--dry-run : Print the metrics without publishing or draining them}';

    /**
     * @var string
     */
    protected $description = 'Sample queue depth and publish the accumulated queue metrics';

    public function handle(QueueMonitor $monitor, Collector $collector, Counters $counters, CacheFactory $cache): int {
        if (! $monitor->enabled()) {
            $this->components->warn('Queue monitoring is disabled.');

            return self::SUCCESS;
        }

        $pairs = $this->pairs($monitor);

        if ($this->option('dry-run') === true) {
            return $this->sample($pairs, $collector, $counters, null);
        }

        // The locks live in the counter store because that is what a second
        // sampler would drain twice; it is shared by construction.
        $store = $cache->store($monitor->counterStore())->getStore();

        if (! $store instanceof LockProvider) {
            $this->fail('The counter store cannot take the sampler lock.');
        }

        return $this->sample(
            $pairs,
            $collector,
            $counters,
            $this->laravel->make(MetricSink::class),
            fn (string $connection, string $queue): Lock => $store->lock(
                $monitor->counterPrefix().':sample:'.hash('xxh128', $connection."\0".$queue),
                self::LOCK_SECONDS,
            ),
        );
    }

    /**
     * One broken or slow queue must not blank the others, so each pair stands
     * alone, under a lock of its own.
     *
     * @param list<array{0: string, 1: string}>    $pairs
     * @param (Closure(string, string): Lock)|null $lockFor
     */
    private function sample(array $pairs, Collector $collector, Counters $counters, ?MetricSink $sink, ?Closure $lockFor = null): int {
        $status = self::SUCCESS;

        foreach ($pairs as [$connection, $queue]) {
            $lock = null;

            try {
                $candidate = $lockFor === null ? null : $lockFor($connection, $queue);

                if ($candidate !== null && ! $candidate->get()) {
                    $this->components->warn("{$connection}:{$queue} is being sampled by another run; skipping.");

                    continue;
                }

                $lock = $candidate;
                [$metrics, $readings] = $collector->collect($connection, $queue);

                if ($sink === null) {
                    $this->render($metrics);

                    continue;
                }

                // A run that outlived its lock may be racing a newer one for
                // these counters; never drain them twice. MySQL reports a
                // refresh within the same second as changing no rows, so
                // ownership decides.
                if ($lock instanceof CacheLock && ! $lock->refresh() && ! $lock->isOwnedByCurrentProcess()) {
                    $status = self::FAILURE;
                    $this->components->error("{$connection}:{$queue}: lost the sampler lock; not draining it.");

                    continue;
                }

                $sink->write($metrics);
                $counters->commit($connection, $queue, $readings);
            } catch (Throwable $e) {
                $status = self::FAILURE;

                $this->report($e);
                $this->components->error("{$connection}:{$queue}: {$e->getMessage()}");
            } finally {
                try {
                    $lock?->release();
                } catch (Throwable $e) {
                    $this->report($e);
                }
            }
        }

        return $status;
    }

    private function report(Throwable $e): void {
        $this->laravel->make(ExceptionHandler::class)->report($e);
    }

    /**
     * @return list<array{0: string, 1: string}>
     */
    private function pairs(QueueMonitor $monitor): array {
        $argument = $this->argument('queues');

        if (! is_string($argument) || trim($argument) === '') {
            return $monitor->sampledQueues();
        }

        $pairs = [];

        foreach (explode(',', $argument) as $entry) {
            $entry = trim($entry);

            if ($entry === '') {
                continue;
            }

            [$connection, $queue] = str_contains($entry, ':')
                ? explode(':', $entry, 2)
                : [$monitor->defaultConnection(), $entry];

            $connection = $monitor->innerConnection($connection);

            if ($connection === '' || $queue === '') {
                $this->fail("Invalid queue [{$entry}]: expected connection:queue, or a queue on the default connection.");
            }

            $pairs[] = [$connection, $queue];
        }

        return array_values(array_unique($pairs, SORT_REGULAR));
    }

    /**
     * Shows exactly what would be published, which also keeps producer
     * supplied names from writing control sequences to the terminal.
     *
     * @param list<Metric> $metrics
     */
    private function render(array $metrics): void {
        foreach ($metrics as $metric) {
            $dimensions = [];

            foreach ($metric->dimensions as $name => $value) {
                $dimensions[] = "{$name}=".EmfDocument::sanitise($value);
            }

            $this->components->twoColumnDetail(
                $metric->name.' '.implode(' ', $dimensions),
                $metric->value.' '.$metric->unit->value,
            );
        }
    }
}
