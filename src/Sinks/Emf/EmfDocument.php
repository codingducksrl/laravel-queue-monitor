<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Sinks\Emf;

use CodingDuck\QueueMonitor\Metric;
use InvalidArgumentException;
use JsonSerializable;

/**
 * One EMF document describes exactly one dimension tuple: every metric in a
 * directive is published against every dimension set in it, and dimension
 * values are single-valued root members.
 */
final readonly class EmfDocument implements JsonSerializable {
    public const int MAX_METRICS = 100;

    public const int MAX_DIMENSIONS = 30;

    public const int MAX_NAME = 1024;

    public const int MAX_DIMENSION_NAME = 250;

    public const int MAX_DIMENSION_VALUE = 1024;

    public const string UNKNOWN = '__unknown__';

    /**
     * @param array<string, string> $dimensions
     * @param list<Metric>          $metrics
     * @param array<string, string> $entity
     */
    public function __construct(
        public string $namespace,
        public int $timestamp,
        public array $dimensions,
        public array $metrics,
        public array $entity = [],
    ) {
        if ($namespace === '' || mb_strlen($namespace) > self::MAX_NAME) {
            throw new InvalidArgumentException("EMF namespace [{$namespace}] must be 1-".self::MAX_NAME.' characters.');
        }

        if (count($dimensions) > self::MAX_DIMENSIONS) {
            throw new InvalidArgumentException('An EMF dimension set holds at most '.self::MAX_DIMENSIONS.' keys.');
        }

        foreach (array_keys($dimensions) as $name) {
            if ($name === '' || mb_strlen($name) > self::MAX_DIMENSION_NAME) {
                throw new InvalidArgumentException("EMF dimension name [{$name}] must be 1-".self::MAX_DIMENSION_NAME.' characters.');
            }
        }

        foreach ($metrics as $metric) {
            if ($metric->name === '' || mb_strlen($metric->name) > self::MAX_NAME) {
                throw new InvalidArgumentException("Metric name [{$metric->name}] must be 1-".self::MAX_NAME.' characters.');
            }
        }
    }

    /**
     * Split so no part exceeds the directive's metric cap. Each metric lands in
     * exactly one part, so a split can never double publish.
     *
     * @return list<self>
     */
    public function chunk(): array {
        if (count($this->metrics) <= self::MAX_METRICS) {
            return [$this];
        }

        $parts = [];

        foreach (array_chunk($this->metrics, self::MAX_METRICS) as $chunk) {
            $parts[] = new self($this->namespace, $this->timestamp, $this->dimensions, $chunk, $this->entity);
        }

        return $parts;
    }

    /**
     * @return array<string, mixed>
     */
    public function jsonSerialize(): array {
        $definitions = [];
        $values = [];

        foreach ($this->metrics as $metric) {
            $definitions[] = ['Name' => $metric->name, 'Unit' => $metric->unit->value];
            $values[$metric->name] = $metric->value;
        }

        $dimensions = [];

        foreach ($this->dimensions as $name => $value) {
            $dimensions[$name] = self::sanitise($value);
        }

        return [
            '_aws' => [
                'Timestamp' => $this->timestamp,
                'CloudWatchMetrics' => [[
                    'Namespace' => $this->namespace,
                    'Dimensions' => [array_keys($this->dimensions)],
                    'Metrics' => $definitions,
                ]],
            ],
            ...$this->entity,
            ...$dimensions,
            ...$values,
        ];
    }

    public function toJson(): string {
        return json_encode($this, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION);
    }

    /**
     * CloudWatch drops the whole record for an empty value or a control
     * character, taking every sibling metric with it.
     */
    public static function sanitise(string $value): string {
        $clean = trim((string) preg_replace('/[\x00-\x1F\x7F]/u', '', $value));

        if ($clean === '') {
            return self::UNKNOWN;
        }

        return mb_strlen($clean) > self::MAX_DIMENSION_VALUE
            ? mb_substr($clean, 0, self::MAX_DIMENSION_VALUE)
            : $clean;
    }
}
