<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Sinks;

use CodingDuck\QueueMonitor\Metric;
use CodingDuck\QueueMonitor\MetricSink;

final class NullSink implements MetricSink {
    /**
     * @param list<Metric> $metrics
     */
    public function write(array $metrics): void {}
}
