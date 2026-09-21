<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Tests\Support;

use CodingDuck\QueueMonitor\Sinks\Emf\Emitter;

final class FakeEmitter implements Emitter {
    /** @var list<string> */
    public array $lines = [];

    public function emit(string $line): void {
        $this->lines[] = $line;
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function documents(): array {
        return array_map(
            static function (string $line): array {
                /** @var array<string, mixed> $decoded */
                $decoded = json_decode($line, true, 512, JSON_THROW_ON_ERROR);

                return $decoded;
            },
            $this->lines,
        );
    }
}
