<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

final readonly class Metric {
    /**
     * @param array<string, string> $dimensions
     */
    public function __construct(
        public string $name,
        public int|float $value,
        public Unit $unit,
        public array $dimensions,
    ) {}

    /**
     * @param array<string, string> $dimensions
     */
    public static function make(string $name, int|float $value, array $dimensions, Unit $unit = Unit::Count): self {
        return new self($name, $value, $unit, $dimensions);
    }
}
