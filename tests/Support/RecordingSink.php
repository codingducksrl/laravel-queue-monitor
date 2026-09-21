<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Tests\Support;

use CodingDuck\QueueMonitor\Metric;
use CodingDuck\QueueMonitor\MetricSink;

final class RecordingSink implements MetricSink {
    /** @var list<Metric> */
    public array $metrics = [];

    /**
     * @param list<Metric> $metrics
     */
    public function write(array $metrics): void {
        $this->metrics = [...$this->metrics, ...$metrics];
    }

    public function value(string $name, ?string $class = null): int|float|null {
        foreach ($this->metrics as $metric) {
            if ($metric->name === $name && ($metric->dimensions['JobClass'] ?? null) === $class) {
                return $metric->value;
            }
        }

        return null;
    }

    /**
     * @return list<string>
     */
    public function classesFor(string $name): array {
        $classes = [];

        foreach ($this->metrics as $metric) {
            if ($metric->name === $name && isset($metric->dimensions['JobClass'])) {
                $classes[] = $metric->dimensions['JobClass'];
            }
        }

        return $classes;
    }
}
