<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Console\Commands;

use CodingDuck\QueueMonitor\Collector;
use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\Metric;
use CodingDuck\QueueMonitor\MetricSink;
use CodingDuck\QueueMonitor\QueueMonitor;
use CodingDuck\QueueMonitor\Sinks\EmfSink;
use Illuminate\Console\Command;
use Illuminate\Contracts\Debug\ExceptionHandler;
use RuntimeException;
use Symfony\Component\Console\Output\OutputInterface;
use Throwable;

final class SampleCommand extends Command {
    /**
     * Well above any sane read of one queue, and refreshed before that queue
     * is drained. A killed run delays only the queue it held.
     */
    private const int LOCK_SECONDS = 300;

    protected $signature = 'queue-monitor:sample
        {queues? : Comma separated connection:queue pairs, defaulting to the configured ones}
        {--dry-run : Print the metrics without publishing or draining them}';

    protected $description = 'Sample queue depth and publish the accumulated queue metrics';

    public function handle(QueueMonitor $monitor): int {
        if (! $monitor->enabled()) {
            $this->components->warn('Queue monitoring is disabled.');

            return self::SUCCESS;
        }

        $pairs = $this->pairs($monitor);
        $collector = $this->laravel->make(Collector::class);
        $counters = $this->laravel->make(Counters::class);
        $sink = $this->option('dry-run') === true ? null : $this->laravel->make(MetricSink::class);
        $status = self::SUCCESS;

        foreach ($pairs as [$connection, $queue]) {
            try {
                if ($sink === null) {
                    $this->render($collector->collect($connection, $queue)[0]);

                    continue;
                }

                $lock = $counters->lock($connection, $queue, self::LOCK_SECONDS);

                $sampled = $lock->get(function () use ($lock, $collector, $counters, $sink, $connection, $queue): bool {
                    [$metrics, $readings] = $collector->collect($connection, $queue);

                    // A run that outlived its lock may be racing a newer one; never drain twice.
                    if (! $lock->refresh()) {
                        throw new RuntimeException('Lost the sampler lock; not draining it.');
                    }

                    $sink->write($metrics);
                    $counters->commit($connection, $queue, $readings);

                    return true;
                });

                if ($sampled === false) {
                    $this->components->warn("{$connection}:{$queue} is being sampled by another run; skipping.");
                }
            } catch (Throwable $e) {
                $status = self::FAILURE;

                $this->laravel->make(ExceptionHandler::class)->report($e);
                $this->components->error("{$connection}:{$queue}: {$e->getMessage()}");
            }
        }

        return $status;
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
     * Raw, so dimension values print exactly as the EMF sink publishes them.
     *
     * @param list<Metric> $metrics
     */
    private function render(array $metrics): void {
        foreach ($metrics as $metric) {
            $dimensions = [];

            foreach ($metric->dimensions as $name => $value) {
                $dimensions[] = "{$name}=".EmfSink::sanitise($value);
            }

            $this->output->writeln($metric->name.' '.implode(' ', $dimensions).' '.$metric->value, OutputInterface::OUTPUT_RAW);
        }
    }
}
