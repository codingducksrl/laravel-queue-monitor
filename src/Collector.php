<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use Illuminate\Contracts\Queue\Factory;
use Illuminate\Queue\Failed\CountableFailedJobProvider;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Queue\Jobs\InspectedJob;
use Illuminate\Support\Collection;

final readonly class Collector {
    public const string OTHER = '__other__';

    public function __construct(
        private Factory $queues,
        private Counters $counters,
        private FailedJobProviderInterface $failer,
        private int $maxClasses = 25,
    ) {}

    /**
     * Returns the metrics to publish and the counter readings they were built
     * from. The caller commits the readings only once the sink has accepted
     * them, so a failed write loses nothing.
     *
     * @return array{0: list<Metric>, 1: array<string, array<string, int>>}
     */
    public function collect(string $connection, string $queue): array {
        $dimensions = ['Connection' => $connection, 'Queue' => $queue];
        $driver = $this->queues->connection($connection);

        $metrics = [
            Metric::make(MetricName::JobsPending, $driver->pendingSize($queue), $dimensions),
            Metric::make(MetricName::JobsDelayed, $driver->delayedSize($queue), $dimensions),
            Metric::make(MetricName::JobsInProgress, $driver->reservedSize($queue), $dimensions),
        ];

        if ($this->failer instanceof CountableFailedJobProvider) {
            $metrics[] = Metric::make(
                MetricName::FailedJobsTotal, (int) $this->failer->count($connection, $queue), $dimensions
            );
        }

        foreach ($this->reservedByClass($driver, $queue) as $class => $count) {
            $metrics[] = Metric::make(MetricName::JobsInProgress, $count, [...$dimensions, 'JobClass' => $class]);
        }

        $readings = $this->counters->read($connection, $queue, MetricName::counters());

        foreach ($this->fold($readings) as $metric => $classes) {
            $metrics[] = Metric::make($metric, array_sum($classes), $dimensions);

            foreach ($classes as $class => $value) {
                $metrics[] = Metric::make($metric, $value, [...$dimensions, 'JobClass' => $class]);
            }
        }

        return [$metrics, $readings];
    }

    /**
     * Keep the busiest classes and relabel the rest, so the per-class values
     * still sum to the queue level total.
     *
     * @param array<string, array<string, int>> $readings
     *
     * @return array<string, array<string, int>>
     */
    private function fold(array $readings): array {
        $totals = [];

        foreach ($readings as $classes) {
            foreach ($classes as $class => $value) {
                $totals[$class] = ($totals[$class] ?? 0) + $value;
            }
        }

        if (count($totals) <= $this->maxClasses) {
            return $readings;
        }

        uksort($totals, fn (string $a, string $b): int => [$totals[$b], $a] <=> [$totals[$a], $b]);

        $keep = array_slice(array_keys($totals), 0, $this->maxClasses);
        $folded = [];

        foreach ($readings as $metric => $classes) {
            foreach ($classes as $class => $value) {
                $label = in_array($class, $keep, true) ? $class : self::OTHER;

                $folded[$metric][$label] = ($folded[$metric][$label] ?? 0) + $value;
            }
        }

        return $folded;
    }

    /**
     * Bounded by the number of workers, so cheap. Drivers that cannot inspect
     * jobs return an empty list and no per-class metric is published.
     *
     * @return array<string, int>
     */
    private function reservedByClass(object $driver, string $queue): array {
        if (! method_exists($driver, 'reservedJobs')) {
            return [];
        }

        /** @var mixed $reserved */
        $reserved = $driver->reservedJobs($queue);

        if (! $reserved instanceof Collection) {
            return [];
        }

        $counts = [];

        foreach ($reserved as $job) {
            if ($job instanceof InspectedJob && $job->name !== null) {
                $counts[$job->name] = ($counts[$job->name] ?? 0) + 1;
            }
        }

        return $counts;
    }
}
