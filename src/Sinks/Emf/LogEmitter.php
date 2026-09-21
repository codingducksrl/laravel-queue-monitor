<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Sinks\Emf;

use Illuminate\Log\LogManager;

final readonly class LogEmitter implements Emitter {
    public function __construct(
        private LogManager $log,
        private ?string $channel = null,
        private string $level = 'info',
    ) {}

    public function emit(string $line): void {
        $this->log->channel($this->channel)->log($this->level, $line);
    }
}
