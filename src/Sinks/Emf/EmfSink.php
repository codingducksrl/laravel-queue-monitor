<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Sinks\Emf;

use CodingDuck\QueueMonitor\Metric;
use CodingDuck\QueueMonitor\MetricSink;
use Illuminate\Support\Carbon;

final readonly class EmfSink implements MetricSink {
    /**
     * @param array<string, string> $entity
     */
    public function __construct(
        private Emitter $emitter,
        private string $namespace,
        private array $entity = [],
    ) {}

    /**
     * @param list<Metric> $metrics
     */
    public function write(array $metrics): void {
        $timestamp = Carbon::now()->getTimestampMs();

        foreach ($this->group($metrics) as [$dimensions, $grouped]) {
            $document = new EmfDocument($this->namespace, $timestamp, $dimensions, $grouped, $this->entity);

            foreach ($document->chunk() as $part) {
                $this->emitter->emit($part->toJson());
            }
        }
    }

    /**
     * @param list<Metric> $metrics
     *
     * @return list<array{0: array<string, string>, 1: list<Metric>}>
     */
    private function group(array $metrics): array {
        /** @var array<string, array{0: array<string, string>, 1: list<Metric>}> $groups */
        $groups = [];

        foreach ($metrics as $metric) {
            $key = json_encode($metric->dimensions, JSON_THROW_ON_ERROR);

            $groups[$key][0] = $metric->dimensions;
            $groups[$key][1][] = $metric;
        }

        return array_values($groups);
    }
}
