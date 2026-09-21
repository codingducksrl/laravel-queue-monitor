<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

interface MetricSink {
    /**
     * @param list<Metric> $metrics
     */
    public function write(array $metrics): void;
}
