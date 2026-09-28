<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

final readonly class Metric {
    /**
     * @param array<string, string> $dimensions
     */
    public function __construct(
        public string $name,
        public int $value,
        public array $dimensions,
    ) {}
}
