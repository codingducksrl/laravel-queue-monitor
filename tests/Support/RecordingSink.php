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

    public function value(string $name, ?string $class = null): ?int {
        $matches = array_values(array_filter(
            $this->metrics,
            fn (Metric $metric): bool => $metric->name === $name && ($metric->dimensions['JobClass'] ?? null) === $class,
        ));

        expect(count($matches))->toBeLessThanOrEqual(1);

        return $matches[0]->value ?? null;
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
